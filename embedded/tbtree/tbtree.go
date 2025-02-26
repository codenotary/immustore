/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tbtree

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded"
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrIllegalArguments              = fmt.Errorf("tbtree: %w", embedded.ErrIllegalArguments)
	ErrInvalidOptions                = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
	ErrorPathIsNotADirectory         = errors.New("tbtree: path is not a directory")
	ErrReadingFileContent            = errors.New("tbtree: error reading required file content")
	ErrKeyNotFound                   = fmt.Errorf("tbtree: %w", embedded.ErrKeyNotFound)
	ErrorMaxKeySizeExceeded          = errors.New("tbtree: max key size exceeded")
	ErrorMaxValueSizeExceeded        = errors.New("tbtree: max value size exceeded")
	ErrOffsetOutOfRange              = fmt.Errorf("tbtree: %w", embedded.ErrOffsetOutOfRange)
	ErrIllegalState                  = embedded.ErrIllegalState // TODO: grpc error mapping hardly relies on the actual message, see IllegalStateHandlerInterceptor
	ErrAlreadyClosed                 = errors.New("tbtree: index already closed")
	ErrSnapshotsNotClosed            = errors.New("tbtree: snapshots not closed")
	ErrorToManyActiveSnapshots       = errors.New("tbtree: max active snapshots limit reached")
	ErrCorruptedFile                 = errors.New("tbtree: file is corrupted")
	ErrCorruptedCLog                 = errors.New("tbtree: commit log is corrupted")
	ErrCompactAlreadyInProgress      = errors.New("tbtree: compact already in progress")
	ErrCompactionThresholdNotReached = errors.New("tbtree: compaction threshold not yet reached")
	ErrIncompatibleDataFormat        = errors.New("tbtree: incompatible data format")
	ErrTargetPathAlreadyExists       = errors.New("tbtree: target folder already exists")
	ErrNoMoreEntries                 = fmt.Errorf("tbtree: %w", embedded.ErrNoMoreEntries)
	ErrReadersNotClosed              = errors.New("tbtree: readers not closed")
)

const Version = 3

const (
	MetaVersion      = "VERSION"
	MetaMaxNodeSize  = "MAX_NODE_SIZE"
	MetaMaxKeySize   = "MAX_KEY_SIZE"
	MetaMaxValueSize = "MAX_VALUE_SIZE"
)

const (
	// actual nodes and commit folders will be suffixed by root logical timestamp except for initial trees so to be backward compatible
	nodesFolderPrefix  = "nodes"
	commitFolderPrefix = "commit"

	historyFolder = "history" // history data is snapshot-agnostic / compaction-agnostic i.e. history(t) = history(compact(t))
)

// initial and final nLog size, root node size, nLog digest since initial and final points
// initial and final hLog size, hLog digest since initial and final points
const cLogEntrySize = 8 + 8 + 4 + sha256.Size + 8 + 8 + sha256.Size

type cLogEntry struct {
	synced bool

	initialNLogSize int64
	finalNLogSize   int64
	rootNodeSize    int
	nLogChecksum    [sha256.Size]byte

	initialHLogSize int64
	finalHLogSize   int64
	hLogChecksum    [sha256.Size]byte
}

func (e *cLogEntry) serialize() []byte {
	var b [cLogEntrySize]byte

	i := 0

	binary.BigEndian.PutUint64(b[i:], uint64(e.initialNLogSize))
	if !e.synced {
		b[i] |= 0x80 // async flag in the msb is set
	}
	i += 8

	binary.BigEndian.PutUint64(b[i:], uint64(e.finalNLogSize))
	i += 8

	binary.BigEndian.PutUint32(b[i:], uint32(e.rootNodeSize))
	i += 4

	copy(b[i:], e.nLogChecksum[:])
	i += sha256.Size

	binary.BigEndian.PutUint64(b[i:], uint64(e.initialHLogSize))
	i += 8

	binary.BigEndian.PutUint64(b[i:], uint64(e.finalHLogSize))
	i += 8

	copy(b[i:], e.hLogChecksum[:])
	i += sha256.Size

	return b[:]
}

func (e *cLogEntry) isValid() bool {
	return e.initialNLogSize <= e.finalNLogSize &&
		e.rootNodeSize > 0 &&
		int64(e.rootNodeSize) <= e.finalNLogSize &&
		e.initialHLogSize <= e.finalHLogSize
}

func (e *cLogEntry) deserialize(b []byte) {
	e.synced = b[0]&0x80 == 0
	b[0] &= 0x7F // remove syncing flag

	i := 0

	e.initialNLogSize = int64(binary.BigEndian.Uint64(b[i:]))
	i += 8

	e.finalNLogSize = int64(binary.BigEndian.Uint64(b[i:]))
	i += 8

	e.rootNodeSize = int(binary.BigEndian.Uint32(b[i:]))
	i += 4

	copy(e.nLogChecksum[:], b[i:])
	i += sha256.Size

	e.initialHLogSize = int64(binary.BigEndian.Uint64(b[i:]))
	i += 8

	e.finalHLogSize = int64(binary.BigEndian.Uint64(b[i:]))
	i += 8

	copy(e.hLogChecksum[:], b[i:])
	i += sha256.Size
}

// TBTree implements a timed-btree
type TBtree struct {
	path string
	id   uint16

	logger logger.Logger

	nLog   appendable.Appendable
	cache  *cache.Cache
	nmutex sync.Mutex // mutex for cache and file reading

	hLog appendable.Appendable

	cLog appendable.Appendable

	root node

	maxNodeSize                int
	insertionCountSinceFlush   int
	insertionCountSinceSync    int
	insertionCountSinceCleanup int
	flushThld                  int
	maxBufferedDataSize        int
	syncThld                   int
	flushBufferSize            int
	cleanupPercentage          float32
	maxActiveSnapshots         int
	renewSnapRootAfter         time.Duration
	readOnly                   bool
	cacheSize                  int
	fileSize                   int
	fileMode                   os.FileMode
	maxKeySize                 int
	maxValueSize               int
	compactionThld             int
	delayDuringCompaction      time.Duration
	nodesLogMaxOpenedFiles     int
	historyLogMaxOpenedFiles   int
	commitLogMaxOpenedFiles    int
	appFactory                 AppFactoryFunc
	appRemove                  AppRemoveFunc

	bufferedDataSize int
	onFlush          OnFlushFunc
	snapshots        map[uint64]*Snapshot
	maxSnapshotID    uint64
	lastSnapRoot     node
	lastSnapRootAt   time.Time

	committedLogSize  int64
	committedNLogSize int64
	committedHLogSize int64
	minOffset         int64

	compacting bool

	closed  bool
	rwmutex sync.RWMutex
}

type path []*pathNode

type pathNode struct {
	node   *innerNode
	offset int
}

type node interface {
	insert(kvts []*KVT) ([]node, int, error)
	get(key []byte) (value []byte, ts uint64, hc uint64, err error)
	getBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error)
	history(key []byte, offset uint64, descOrder bool, limit int) ([]TimedValue, uint64, error)
	findLeafNode(seekKey []byte, path path, offset int, neqKey []byte, descOrder bool) (path, *leafNode, int, error)
	minKey() []byte
	ts() uint64
	setTs(ts uint64) (node, error)
	tsMutated() bool
	size() (int, error)
	mutated() bool
	offset() int64    // only valid when !mutated()
	minOffset() int64 // only valid when !mutated()
	writeTo(nw, hw io.Writer, writeOpts *WriteOpts, buf []byte) (nOff, minOff int64, wN, wH int64, err error)
}

type writeProgressOutputFunc func(innerNodesWritten int, leafNodesWritten int, entriesWritten int)
type writeFinnishOutputFunc func()

type WriteOpts struct {
	OnlyMutated    bool
	BaseNLogOffset int64
	BaseHLogOffset int64
	commitLog      bool
	reportProgress writeProgressOutputFunc
	MinOffset      int64
}

type innerNode struct {
	t       *TBtree
	nodes   []node
	_ts     uint64
	off     int64
	_minOff int64
	mut     bool
}

type leafNode struct {
	t      *TBtree
	values []*leafValue
	_ts    uint64
	off    int64
	mut    bool
}

type nodeRef struct {
	t       *TBtree
	_minKey []byte
	_ts     uint64
	off     int64
	_minOff int64
}

type leafValue struct {
	key         []byte
	timedValues []TimedValue
	hOff        int64
	hCount      uint64
}

type TimedValue struct {
	Value []byte
	Ts    uint64
}

func Open(path string, opts *Options) (*TBtree, error) {
	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		err = os.Mkdir(path, opts.fileMode)
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaMaxNodeSize, opts.maxNodeSize)
	metadata.PutInt(MetaMaxKeySize, opts.maxKeySize)
	metadata.PutInt(MetaMaxValueSize, opts.maxValueSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.readOnly).
		WithRetryableSync(false).
		WithFileSize(opts.fileSize).
		WithFileMode(opts.fileMode).
		WithWriteBufferSize(opts.flushBufferSize).
		WithMetadata(metadata.Bytes())

	appFactory := opts.appFactory
	if appFactory == nil {
		appFactory = func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			path := filepath.Join(rootPath, subPath)
			return multiapp.Open(path, opts)
		}
	}

	appRemove := opts.appRemove
	if appRemove == nil {
		appRemove = func(rootPath, subPath string) error {
			path := filepath.Join(rootPath, subPath)
			return os.RemoveAll(path)
		}
	}

	appendableOpts.WithFileExt("hx")
	appendableOpts.WithMaxOpenedFiles(opts.historyLogMaxOpenedFiles)
	hLog, err := appFactory(path, historyFolder, appendableOpts)
	if err != nil {
		return nil, err
	}

	// If compaction was not fully completed, a valid or partially written full snapshot may be there
	snapIDs, err := recoverFullSnapshots(path, commitFolderPrefix, opts.logger)
	if err != nil {
		return nil, err
	}

	// Try snapshots from newest to older
	for i := len(snapIDs); i > 0; i-- {
		snapID := snapIDs[i-1]

		nFolder := snapFolder(nodesFolderPrefix, snapID)
		cFolder := snapFolder(commitFolderPrefix, snapID)

		snapPath := filepath.Join(path, cFolder)

		opts.logger.Infof("reading snapshots at '%s'...", snapPath)

		appendableOpts.WithFileExt("n")
		appendableOpts.WithMaxOpenedFiles(opts.nodesLogMaxOpenedFiles)
		nLog, err := appFactory(path, nFolder, appendableOpts)
		if err != nil {
			opts.logger.Infof("skipping snapshots at '%s', reading node data returned: %v", snapPath, err)
			continue
		}

		appendableOpts.WithFileExt("ri")
		appendableOpts.WithMaxOpenedFiles(opts.commitLogMaxOpenedFiles)
		cLog, err := appFactory(path, cFolder, appendableOpts)
		if err != nil {
			nLog.Close()
			opts.logger.Infof("skipping snapshots at '%s', reading commit data returned: %v", snapPath, err)
			continue
		}

		var t *TBtree
		var discardSnapshotsFolder bool

		cLogSize, err := cLog.Size()
		if err == nil && cLogSize < cLogEntrySize {
			opts.logger.Infof("skipping snapshots at '%s', reading commit data returned: %s", snapPath, "empty clog")
			discardSnapshotsFolder = true
		}
		if err == nil && !discardSnapshotsFolder {
			// TODO: semantic validation and further amendment procedures may be done instead of a full initialization
			t, err = OpenWith(path, nLog, hLog, cLog, opts)
		}
		if err != nil {
			opts.logger.Infof("skipping snapshots at '%s', opening btree returned: %v", snapPath, err)
			discardSnapshotsFolder = true
		}

		if discardSnapshotsFolder {
			nLog.Close()
			cLog.Close()

			err = discardSnapshots(path, snapIDs[i-1:i], appRemove, opts.logger)
			if err != nil {
				opts.logger.Warningf("discarding snapshots at '%s' returned: %v", path, err)
			}

			continue
		}

		opts.logger.Infof("successfully read snapshots at '%s'", snapPath)

		// Discard older snapshots upon successful validation
		err = discardSnapshots(path, snapIDs[:i-1], appRemove, opts.logger)
		if err != nil {
			opts.logger.Warningf("discarding snapshots at '%s' returned: %v", path, err)
		}

		return t, nil
	}

	// No snapshot present or none was valid, fresh initialization

	err = hLog.SetOffset(0)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("n")
	appendableOpts.WithMaxOpenedFiles(opts.nodesLogMaxOpenedFiles)
	nLog, err := appFactory(path, nodesFolderPrefix, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("ri")
	appendableOpts.WithMaxOpenedFiles(opts.commitLogMaxOpenedFiles)
	cLog, err := appFactory(path, commitFolderPrefix, appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(path, nLog, hLog, cLog, opts)
}

func snapFolder(folder string, snapID uint64) string {
	if snapID == 0 {
		return folder
	}

	return fmt.Sprintf("%s%016d", folder, snapID)
}

func recoverFullSnapshots(path, prefix string, logger logger.Logger) (snapIDs []uint64, err error) {
	fis, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	for _, f := range fis {
		if f.IsDir() && strings.HasPrefix(f.Name(), prefix) {
			if f.Name() == prefix {
				snapIDs = append(snapIDs, 0)
				continue
			}

			id, err := strconv.ParseInt(strings.TrimPrefix(f.Name(), prefix), 10, 64)
			if err != nil {
				logger.Warningf("invalid folder found '%s', skipped during index selection", f.Name())
				continue
			}

			snapIDs = append(snapIDs, uint64(id))
		}
	}

	return snapIDs, nil
}

func discardSnapshots(path string, snapIDs []uint64, appRemove AppRemoveFunc, logger logger.Logger) error {
	for _, snapID := range snapIDs {
		nFolder := snapFolder(nodesFolderPrefix, snapID)
		cFolder := snapFolder(commitFolderPrefix, snapID)

		logger.Infof("discarding snapshot with id=%d at '%s'..., %d", snapID, path)

		err := appRemove(path, nFolder)
		if err != nil {
			return err
		}

		err = appRemove(path, cFolder)
		if err != nil {
			return err
		}

		logger.Infof("snapshot with id=%d at '%s' has been discarded, %d", snapID, path)
	}

	return nil
}

func OpenWith(path string, nLog, hLog, cLog appendable.Appendable, opts *Options) (*TBtree, error) {
	if nLog == nil || hLog == nil || cLog == nil {
		return nil, ErrIllegalArguments
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	version, ok := metadata.GetInt(MetaVersion)
	if !ok {
		return nil, ErrCorruptedCLog
	}
	if version < Version {
		return nil, fmt.Errorf("%w: index data was generated using older and incompatible version", ErrIncompatibleDataFormat)
	}

	maxNodeSize, ok := metadata.GetInt(MetaMaxNodeSize)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	maxKeySize, ok := metadata.GetInt(MetaMaxKeySize)
	if !ok {
		maxKeySize = opts.maxKeySize
	}

	maxValueSize, ok := metadata.GetInt(MetaMaxValueSize)
	if !ok {
		maxValueSize = opts.maxValueSize
	}

	if maxNodeSize < requiredNodeSize(maxKeySize, maxValueSize) {
		return nil, fmt.Errorf("%w: max node size is too small for specified max key and max value sizes", ErrIllegalArguments)
	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, err
	}

	rem := cLogSize % cLogEntrySize
	if rem > 0 {
		cLogSize -= rem
		err = cLog.SetOffset(cLogSize)
		if err != nil {
			return nil, err
		}
	}

	nodeCache := opts.cache
	if nodeCache == nil {
		nodeCache, err = cache.NewCache(opts.cacheSize)
		if err != nil {
			return nil, err
		}
	}

	t := &TBtree{
		path:                     path,
		id:                       opts.ID,
		logger:                   opts.logger,
		nLog:                     nLog,
		hLog:                     hLog,
		cLog:                     cLog,
		cache:                    nodeCache,
		maxNodeSize:              maxNodeSize,
		maxKeySize:               maxKeySize,
		maxValueSize:             maxValueSize,
		flushThld:                opts.flushThld,
		maxBufferedDataSize:      opts.maxBufferedDataSize,
		syncThld:                 opts.syncThld,
		flushBufferSize:          opts.flushBufferSize,
		onFlush:                  opts.onFlush,
		cleanupPercentage:        opts.cleanupPercentage,
		renewSnapRootAfter:       opts.renewSnapRootAfter,
		maxActiveSnapshots:       opts.maxActiveSnapshots,
		fileSize:                 opts.fileSize,
		cacheSize:                opts.cacheSize,
		fileMode:                 opts.fileMode,
		compactionThld:           opts.compactionThld,
		delayDuringCompaction:    opts.delayDuringCompaction,
		nodesLogMaxOpenedFiles:   opts.nodesLogMaxOpenedFiles,
		historyLogMaxOpenedFiles: opts.historyLogMaxOpenedFiles,
		commitLogMaxOpenedFiles:  opts.commitLogMaxOpenedFiles,
		readOnly:                 opts.readOnly,
		appFactory:               opts.appFactory,
		appRemove:                opts.appRemove,
		snapshots:                make(map[uint64]*Snapshot),
	}

	var validatedCLogEntry *cLogEntry
	discardedCLogEntries := 0

	// checksum validation up to latest synced entry
	for cLogSize > 0 {
		var b [cLogEntrySize]byte
		n, err := cLog.ReadAt(b[:], cLogSize-cLogEntrySize)
		if errors.Is(err, io.EOF) {
			cLogSize -= int64(n)
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w: while reading index index commit log entry at '%s'", err, path)
		}

		cLogEntry := &cLogEntry{}
		cLogEntry.deserialize(b[:])

		mustDiscard := !cLogEntry.isValid()
		if mustDiscard {
			err = fmt.Errorf("invalid clog entry")
		}

		if !mustDiscard {
			nLogChecksum, nerr := appendable.Checksum(t.nLog, cLogEntry.initialNLogSize, cLogEntry.finalNLogSize-cLogEntry.initialNLogSize)
			if nerr != nil && !errors.Is(nerr, io.EOF) {
				return nil, fmt.Errorf("%w: while calculating nodes log checksum at '%s'", nerr, path)
			}

			hLogChecksum, herr := appendable.Checksum(t.hLog, cLogEntry.initialHLogSize, cLogEntry.finalHLogSize-cLogEntry.initialHLogSize)
			if herr != nil && herr != io.EOF {
				return nil, fmt.Errorf("%w: while calculating history log checksum at '%s'", herr, path)
			}

			mustDiscard = errors.Is(nerr, io.EOF) ||
				errors.Is(herr, io.EOF) ||
				nLogChecksum != cLogEntry.nLogChecksum ||
				hLogChecksum != cLogEntry.hLogChecksum

			err = fmt.Errorf("invalid checksum")
		}

		if mustDiscard {
			t.logger.Infof("discarding snapshots due to %v at '%s'", err, path)

			discardedCLogEntries += int(t.committedLogSize/cLogEntrySize) + 1

			validatedCLogEntry = nil
			t.committedLogSize = 0
		}

		if !mustDiscard && t.committedLogSize == 0 {
			validatedCLogEntry = cLogEntry
			t.committedLogSize = cLogSize
		}

		if !mustDiscard && cLogEntry.synced {
			break
		}

		cLogSize -= cLogEntrySize
	}

	if validatedCLogEntry == nil {
		// It is not necessary to copy the root node when starting with a fresh btree.
		// A fresh root will be used if insertion fails
		t.root = &leafNode{t: t, mut: true}
	} else {
		t.root, err = t.readNodeAt(validatedCLogEntry.finalNLogSize - int64(validatedCLogEntry.rootNodeSize))
		if err != nil {
			return nil, fmt.Errorf("%w: while loading index at '%s'", err, path)
		}

		t.committedNLogSize = validatedCLogEntry.finalNLogSize
		t.committedHLogSize = validatedCLogEntry.finalHLogSize
		t.minOffset = t.root.minOffset()
	}

	metricsBtreeNodesDataBeginOffset.WithLabelValues(t.path).Set(float64(t.minOffset))
	metricsBtreeNodesDataEndOffset.WithLabelValues(t.path).Set(float64(t.committedNLogSize))

	err = t.hLog.SetOffset(t.committedHLogSize)
	if err != nil {
		return nil, fmt.Errorf("%w: while setting initial offset of history log for index '%s'", err, path)
	}

	err = t.cLog.SetOffset(t.committedLogSize)
	if err != nil {
		return nil, fmt.Errorf("%w: while setting initial offset of commit log for index '%s'", err, path)
	}

	opts.logger.Infof("index '%s' {ts=%d, discarded_snapshots=%d} successfully loaded", path, t.Ts(), discardedCLogEntries)

	return t, nil
}

func greatestKeyOfSize(size int) []byte {
	k := make([]byte, size)
	for i := 0; i < size; i++ {
		k[i] = 0xFF
	}
	return k
}

// requiredNodeSize calculates the lower bound for node size
func requiredNodeSize(maxKeySize, maxValueSize int) int {
	// space for at least two children is required for inner nodes
	// 31 bytes are fixed in leafNode serialization while 29 bytes are fixed in innerNodes
	minInnerNode := 2 * (29 + maxKeySize)
	minLeafNode := 31 + maxKeySize + maxValueSize

	if minInnerNode < minLeafNode {
		return minLeafNode
	}

	return minInnerNode
}

func (t *TBtree) GetOptions() *Options {
	return DefaultOptions().
		WithReadOnly(t.readOnly).
		WithFileMode(t.fileMode).
		WithFileSize(t.fileSize).
		WithMaxKeySize(t.maxKeySize).
		WithMaxValueSize(t.maxValueSize).
		WithLogger(t.logger).
		WithCacheSize(t.cacheSize).
		WithFlushThld(t.flushThld).
		WithSyncThld(t.syncThld).
		WithFlushBufferSize(t.flushBufferSize).
		WithCleanupPercentage(t.cleanupPercentage).
		WithMaxActiveSnapshots(t.maxActiveSnapshots).
		WithMaxNodeSize(t.maxNodeSize).
		WithRenewSnapRootAfter(t.renewSnapRootAfter).
		WithCompactionThld(t.compactionThld).
		WithDelayDuringCompaction(t.delayDuringCompaction).
		WithNodesLogMaxOpenedFiles(t.nodesLogMaxOpenedFiles).
		WithHistoryLogMaxOpenedFiles(t.historyLogMaxOpenedFiles).
		WithCommitLogMaxOpenedFiles(t.commitLogMaxOpenedFiles).
		WithAppFactory(t.appFactory).
		WithAppRemoveFunc(t.appRemove)
}

func (t *TBtree) cachePut(n node) {
	t.nmutex.Lock()
	defer t.nmutex.Unlock()

	size, _ := n.size()
	r, _, _ := t.cache.PutWeighted(encodeOffset(t.id, n.offset()), n, size)
	if r != nil {
		metricsCacheEvict.WithLabelValues(t.path).Inc()
	}
}

func encodeOffset(id uint16, offset int64) int64 {
	return int64(id)<<48 | offset
}

func (t *TBtree) nodeAt(offset int64, updateCache bool) (node, error) {
	t.nmutex.Lock()
	defer t.nmutex.Unlock()

	size := t.cache.EntriesCount()
	metricsCacheSizeStats.WithLabelValues(t.path).Set(float64(size))

	encOffset := encodeOffset(t.id, offset)

	v, err := t.cache.Get(encOffset)
	if err == nil {
		metricsCacheHit.WithLabelValues(t.path).Inc()
		return v.(node), nil
	}

	if err == cache.ErrKeyNotFound {
		metricsCacheMiss.WithLabelValues(t.path).Inc()

		n, err := t.readNodeAt(offset)
		if err != nil {
			return nil, err
		}

		if updateCache {
			size, _ := n.size()
			r, _, _ := t.cache.PutWeighted(encOffset, n, size)
			if r != nil {
				metricsCacheEvict.WithLabelValues(t.path).Inc()
			}
		}

		return n, nil
	}

	return nil, err
}

func (t *TBtree) readNodeAt(off int64) (node, error) {
	r := appendable.NewReaderFrom(t.nLog, off, t.maxNodeSize)
	return t.readNodeFrom(r)
}

func (t *TBtree) readNodeFrom(r *appendable.Reader) (node, error) {
	off := r.Offset()

	nodeType, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch nodeType {
	case InnerNodeType:
		n, err := t.readInnerNodeFrom(r)
		if err != nil {
			return nil, err
		}
		n.off = off
		return n, nil
	case LeafNodeType:
		n, err := t.readLeafNodeFrom(r)
		if err != nil {
			return nil, err
		}
		n.off = off
		return n, nil
	}
	return nil, ErrReadingFileContent
}

func (t *TBtree) readInnerNodeFrom(r *appendable.Reader) (*innerNode, error) {
	childCount, err := r.ReadUint16()
	if err != nil {
		return nil, err
	}

	n := &innerNode{
		t:       t,
		nodes:   make([]node, 0, childCount),
		_minOff: math.MaxInt64,
	}

	for c := 0; c < int(childCount); c++ {
		nref, err := t.readNodeRefFrom(r)
		if err != nil {
			return nil, err
		}

		// marker node
		if nref.off == math.MaxInt64 {
			n._ts = nref.ts()
			continue
		}

		n.nodes = append(n.nodes, nref)

		if n._ts < nref._ts {
			n._ts = nref._ts
		}

		if n._minOff > nref._minOff {
			n._minOff = nref._minOff
		}
	}
	return n, nil
}

func (t *TBtree) readNodeRefFrom(r *appendable.Reader) (*nodeRef, error) {
	minKeySize, err := r.ReadUint16()
	if err != nil {
		return nil, err
	}

	minKey := make([]byte, minKeySize)
	_, err = r.Read(minKey)
	if err != nil {
		return nil, err
	}

	ts, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	off, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	minOff, err := r.ReadUint64()
	if err != nil {
		return nil, err
	}

	return &nodeRef{
		t:       t,
		_minKey: minKey,
		_ts:     ts,
		off:     int64(off),
		_minOff: int64(minOff),
	}, nil
}

func (t *TBtree) readLeafNodeFrom(r *appendable.Reader) (*leafNode, error) {
	valueCount, err := r.ReadUint16()
	if err != nil {
		return nil, err
	}

	l := &leafNode{
		t:      t,
		values: make([]*leafValue, 0, valueCount),
	}

	for c := 0; c < int(valueCount); c++ {
		ksize, err := r.ReadUint16()
		if err != nil {
			return nil, err
		}

		if ksize == 0 {
			ts, err := r.ReadUint64()
			if err != nil {
				return nil, err
			}

			l._ts = ts
			continue
		}

		key := make([]byte, ksize)
		_, err = r.Read(key)
		if err != nil {
			return nil, err
		}

		vsize, err := r.ReadUint16()
		if err != nil {
			return nil, err
		}

		value := make([]byte, vsize)
		_, err = r.Read(value)
		if err != nil {
			return nil, err
		}

		ts, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}

		hOff, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}

		hCount, err := r.ReadUint64()
		if err != nil {
			return nil, err
		}

		leafValue := &leafValue{
			key:         key,
			timedValues: []TimedValue{{Value: value, Ts: ts}},
			hOff:        int64(hOff),
			hCount:      hCount,
		}

		l.values = append(l.values, leafValue)

		if l._ts < ts {
			l._ts = ts
		}
	}
	return l, nil
}

func (t *TBtree) Get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	if key == nil {
		return nil, 0, 0, ErrIllegalArguments
	}

	v, ts, hc, err := t.root.get(key)
	return cp(v), ts, hc, err
}

func (t *TBtree) GetBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	if key == nil {
		return nil, 0, 0, ErrIllegalArguments
	}

	return t.root.getBetween(key, initialTs, finalTs)
}

func (t *TBtree) History(key []byte, offset uint64, descOrder bool, limit int) (tvs []TimedValue, hCount uint64, err error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return nil, 0, ErrAlreadyClosed
	}

	if key == nil {
		return nil, 0, ErrIllegalArguments
	}

	if limit < 1 {
		return nil, 0, ErrIllegalArguments
	}

	return t.root.history(key, offset, descOrder, limit)
}

func (t *TBtree) GetWithPrefix(prefix []byte, neq []byte) (key []byte, value []byte, ts uint64, hc uint64, err error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}

	path, leaf, off, err := t.root.findLeafNode(prefix, nil, 0, neq, false)
	if err != nil {
		return nil, nil, 0, 0, err
	}

	metricsBtreeDepth.WithLabelValues(t.path).Set(float64(len(path) + 1))

	leafValue := leaf.values[off]

	if len(prefix) > len(leafValue.key) {
		return nil, nil, 0, 0, ErrKeyNotFound
	}

	if bytes.Equal(prefix, leafValue.key[:len(prefix)]) {
		currValue := leafValue.timedValue()
		return leafValue.key, cp(currValue.Value), currValue.Ts, leafValue.historyCount(), nil
	}

	return nil, nil, 0, 0, ErrKeyNotFound
}

func (t *TBtree) Sync() error {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	_, _, err := t.flushTree(0, true, false, "sync")
	return err
}

func (t *TBtree) Flush() (wN, wH int64, err error) {
	return t.FlushWith(t.cleanupPercentage, false)
}

func (t *TBtree) FlushWith(cleanupPercentage float32, synced bool) (wN, wH int64, err error) {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return 0, 0, ErrAlreadyClosed
	}

	return t.flushTree(cleanupPercentage, synced, true, "flushWith")
}

type appendableWriter struct {
	appendable.Appendable
}

func (aw *appendableWriter) Write(b []byte) (int, error) {
	_, n, err := aw.Append(b)
	return n, err
}

func (t *TBtree) wrapNwarn(formattedMessage string, args ...interface{}) error {
	t.logger.Warningf(formattedMessage, args)
	return fmt.Errorf(formattedMessage, args...)
}

func (t *TBtree) flushTree(cleanupPercentageHint float32, forceSync bool, forceCleanup bool, src string) (wN int64, wH int64, err error) {
	if cleanupPercentageHint < 0 || cleanupPercentageHint > 100 {
		return 0, 0, fmt.Errorf("%w: invalid cleanupPercentage", ErrIllegalArguments)
	}

	cleanupPercentage := cleanupPercentageHint
	if !forceCleanup && t.insertionCountSinceCleanup < t.flushThld {
		cleanupPercentage = 0
	}

	t.logger.Infof("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f, since_cleanup=%d} requested via %s...",
		t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, t.insertionCountSinceCleanup,
		src,
	)

	if !t.root.mutated() && cleanupPercentage == 0 {
		t.logger.Infof("flushing not needed at '%s' {ts=%d, cleanup_percentage=%.2f}", t.path, t.root.ts(), cleanupPercentage)
		return 0, 0, nil
	}

	snapshot := t.newSnapshot(0, t.root)

	// will overwrite partially written and uncommitted data
	// if garbage is accepted then t.committedNLogSize should be set to its size during initialization
	err = t.hLog.SetOffset(t.committedHLogSize)
	if err != nil {
		return 0, 0, err
	}

	err = t.nLog.SetOffset(t.committedNLogSize)
	if err != nil {
		return 0, 0, err
	}

	progressOutputFunc, finishOutputFunc := t.buildWriteProgressOutput(
		metricsFlushedNodesLastCycle,
		metricsFlushedNodesTotal,
		metricsFlushedEntriesLastCycle,
		metricsFlushedEntriesTotal,
		"flushing",
		t.root.ts(),
		time.Minute,
	)
	defer finishOutputFunc()

	expectedNewMinOffset := t.minOffset + int64((float64(t.committedNLogSize-t.minOffset)*float64(cleanupPercentage))/100)

	wopts := &WriteOpts{
		OnlyMutated:    true,
		BaseNLogOffset: t.committedNLogSize,
		BaseHLogOffset: t.committedHLogSize,
		commitLog:      true,
		reportProgress: progressOutputFunc,
		MinOffset:      expectedNewMinOffset,
	}

	_, actualNewMinOffset, wN, wH, err := snapshot.WriteTo(&appendableWriter{t.nLog}, &appendableWriter{t.hLog}, wopts)
	if err != nil {
		return 0, 0, t.wrapNwarn("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
	}

	err = t.hLog.Flush()
	if err != nil {
		return 0, 0, t.wrapNwarn("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
	}

	err = t.nLog.Flush()
	if err != nil {
		return 0, 0, t.wrapNwarn("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
	}

	sync := forceSync || t.insertionCountSinceSync >= t.syncThld

	if sync {
		err = t.hLog.Sync()
		if err != nil {
			return 0, 0, t.wrapNwarn("syncing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
				t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
		}

		err = t.nLog.Sync()
		if err != nil {
			return 0, 0, t.wrapNwarn("syncing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
				t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
		}
	}

	// will overwrite partially written and uncommitted data
	err = t.cLog.SetOffset(t.committedLogSize)
	if err != nil {
		return 0, 0, err
	}

	rootSize, err := t.root.size()
	if err != nil {
		return 0, 0, err
	}

	cLogEntry := &cLogEntry{
		synced: sync,

		initialNLogSize: t.committedNLogSize,
		finalNLogSize:   t.committedNLogSize + wN,
		rootNodeSize:    rootSize,

		initialHLogSize: t.committedHLogSize,
		finalHLogSize:   t.committedHLogSize + wH,
	}

	cLogEntry.nLogChecksum, err = appendable.Checksum(t.nLog, t.committedNLogSize, wN)
	if err != nil {
		return 0, 0, t.wrapNwarn("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
	}

	cLogEntry.hLogChecksum, err = appendable.Checksum(t.hLog, t.committedHLogSize, wH)
	if err != nil {
		return 0, 0, t.wrapNwarn("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
	}

	_, _, err = t.cLog.Append(cLogEntry.serialize())
	if err != nil {
		return 0, 0, t.wrapNwarn("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
	}

	err = t.cLog.Flush()
	if err != nil {
		return 0, 0, t.wrapNwarn("flushing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
	}

	t.insertionCountSinceFlush = 0
	if t.onFlush != nil {
		t.onFlush(t.bufferedDataSize)
	}
	t.bufferedDataSize = 0

	if cleanupPercentage != 0 {
		t.insertionCountSinceCleanup = 0
	}
	t.logger.Infof("index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} successfully flushed",
		t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage)

	if sync {
		err = t.cLog.Sync()
		if err != nil {
			return 0, 0, t.wrapNwarn("syncing index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
				t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
		}

		t.insertionCountSinceSync = 0
		t.logger.Infof("index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} successfully synced",
			t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage)

		// prevent discarding data referenced by opened snapshots
		discardableNLogOffset := actualNewMinOffset
		for _, snap := range t.snapshots {
			if snap.root.minOffset() < discardableNLogOffset {
				discardableNLogOffset = snap.root.minOffset()
			}
		}

		if discardableNLogOffset > t.minOffset {
			t.logger.Infof("discarding unreferenced data at index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f, current_min_offset=%d, new_min_offset=%d}...",
				t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, t.minOffset, actualNewMinOffset)

			err = t.nLog.DiscardUpto(discardableNLogOffset)
			if err != nil {
				t.logger.Warningf("discarding unreferenced data at index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f} returned: %v",
					t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, err)
			}

			metricsBtreeNodesDataBeginOffset.WithLabelValues(t.path).Set(float64(discardableNLogOffset))

			t.logger.Infof("unreferenced data at index '%s' {ts=%d, cleanup_percentage=%.2f/%.2f, current_min_offset=%d, new_min_offset=%d} successfully discarded",
				t.path, t.root.ts(), cleanupPercentageHint, cleanupPercentage, t.minOffset, actualNewMinOffset)
		}

		discardableCommitLogOffset := t.committedLogSize - int64(cLogEntrySize*len(t.snapshots)+1)
		if discardableCommitLogOffset > 0 {
			t.logger.Infof("discarding older snapshots at index '%s' {ts=%d, opened_snapshots=%d}...", t.path, t.root.ts(), len(t.snapshots))

			err = t.cLog.DiscardUpto(discardableCommitLogOffset)
			if err != nil {
				t.logger.Warningf("discarding older snapshots at index '%s' {ts=%d, opened_snapshots=%d} returned: %v", t.path, t.root.ts(), len(t.snapshots), err)
			}

			t.logger.Infof("older snapshots at index '%s' {ts=%d, opened_snapshots=%d} successfully discarded", t.path, t.root.ts(), len(t.snapshots))
		}
	}

	t.minOffset = t.root.minOffset()
	t.committedLogSize += cLogEntrySize
	t.committedNLogSize += wN
	t.committedHLogSize += wH

	metricsBtreeNodesDataEndOffset.WithLabelValues(t.path).Set(float64(t.committedNLogSize))

	// current root can be used as latest snapshot as !t.root.mutated() holds
	t.lastSnapRoot = t.root
	t.lastSnapRootAt = time.Now()

	return wN, wH, nil
}

// SnapshotCount returns the number of stored snapshots
// Note: snapshotCount(compact(t)) = 1
func (t *TBtree) SnapshotCount() (uint64, error) {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	if t.closed {
		return 0, ErrAlreadyClosed
	}

	return t.snapshotCount(), nil
}

func (t *TBtree) snapshotCount() uint64 {
	return uint64(t.committedLogSize / cLogEntrySize)
}

func (t *TBtree) buildWriteProgressOutput(
	nodesLastCycle *prometheus.GaugeVec,
	nodesTotal *prometheus.CounterVec,
	entriesLastCycle *prometheus.GaugeVec,
	entriesTotal *prometheus.CounterVec,
	action string,
	snapTS uint64,
	logReportDelay time.Duration,
) (
	writeProgressOutputFunc,
	writeFinnishOutputFunc,
) {

	iLastCycle := nodesLastCycle.WithLabelValues(t.path, "inner")
	lLastCycle := nodesLastCycle.WithLabelValues(t.path, "leaf")
	eLastCycle := entriesLastCycle.WithLabelValues(t.path)

	iTotal := nodesTotal.WithLabelValues(t.path, "inner")
	lTotal := nodesTotal.WithLabelValues(t.path, "leaf")
	eTotal := entriesTotal.WithLabelValues(t.path)

	innerNodes := 0
	leafNodes := 0
	entries := 0

	lastProgressTime := time.Now()
	progressFunc := func(innerNodesWritten, leafNodesWritten, entriesWritten int) {

		innerNodes += innerNodesWritten
		leafNodes += leafNodesWritten
		entries += entriesWritten

		iTotal.Add(float64(innerNodesWritten))
		lTotal.Add(float64(leafNodesWritten))
		eTotal.Add(float64(entriesWritten))

		now := time.Now()
		if now.Sub(lastProgressTime) > logReportDelay {
			t.logger.Infof(
				"%s index '%s' {ts=%d} progress: %d inner nodes, %d leaf nodes, %d entries...",
				action, t.path, snapTS, leafNodes, innerNodes, entries,
			)
			lastProgressTime = now
		}
	}

	finishFunc := func() {
		iLastCycle.Set(float64(innerNodes))
		lLastCycle.Set(float64(leafNodes))
		eLastCycle.Set(float64(entries))

		t.logger.Infof(
			"%s index '%s' {ts=%d} finished with: %d inner nodes, %d leaf nodes, %d entries",
			action, t.path, snapTS, leafNodes, innerNodes, entries,
		)
	}

	return progressFunc, finishFunc
}

func (t *TBtree) Compact() (uint64, error) {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return 0, ErrAlreadyClosed
	}

	if t.compacting {
		return 0, ErrCompactAlreadyInProgress
	}

	if t.snapshotCount() < uint64(t.compactionThld) {
		return 0, ErrCompactionThresholdNotReached
	}

	_, _, err := t.flushTree(0, false, false, "compact")
	if err != nil {
		return 0, err
	}

	snap := t.newSnapshot(0, t.root)

	t.compacting = true
	defer func() {
		t.compacting = false
	}()

	// snapshot dumping without lock
	t.rwmutex.Unlock()
	defer t.rwmutex.Lock()

	t.logger.Infof("dumping index '%s' {ts=%d}...", t.path, snap.Ts())

	progressOutput, finishOutput := t.buildWriteProgressOutput(
		metricsCompactedNodesLastCycle,
		metricsCompactedNodesTotal,
		metricsCompactedEntriesLastCycle,
		metricsCompactedEntriesTotal,
		"dumping",
		snap.Ts(),
		time.Minute,
	)
	defer finishOutput()

	err = t.fullDump(snap, progressOutput)
	if err != nil {
		return 0, t.wrapNwarn("dumping index '%s' {ts=%d} returned: %v", t.path, snap.Ts(), err)
	}

	t.logger.Infof("index '%s' {ts=%d} successfully dumped", t.path, snap.Ts())

	return snap.Ts(), nil
}

func (t *TBtree) fullDump(snap *Snapshot, progressOutput writeProgressOutputFunc) error {
	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(MetaVersion, Version)
	metadata.PutInt(MetaMaxNodeSize, t.maxNodeSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(false).
		WithRetryableSync(false).
		WithFileSize(t.fileSize).
		WithFileMode(t.fileMode).
		WithWriteBufferSize(t.flushBufferSize).
		WithMetadata(t.cLog.Metadata())

	appendableOpts.WithFileExt("n")
	nLogPath := filepath.Join(t.path, snapFolder(nodesFolderPrefix, snap.Ts()))
	nLog, err := multiapp.Open(nLogPath, appendableOpts)
	if err != nil {
		return err
	}
	defer func() {
		nLog.Close()
	}()

	appendableOpts.WithFileExt("ri")
	cLogPath := filepath.Join(t.path, snapFolder(commitFolderPrefix, snap.Ts()))

	_, err = os.Stat(cLogPath)
	if err == nil {
		return fmt.Errorf("%w: while dumping index to '%s'", ErrTargetPathAlreadyExists, cLogPath)
	}

	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	if err != nil {
		return err
	}
	defer func() {
		cLog.Close()
	}()

	return t.fullDumpTo(snap, nLog, cLog, progressOutput)
}

func (t *TBtree) fullDumpTo(snapshot *Snapshot, nLog, cLog appendable.Appendable, progressOutput writeProgressOutputFunc) error {
	wopts := &WriteOpts{
		OnlyMutated:    false,
		BaseNLogOffset: 0,
		BaseHLogOffset: 0,
		reportProgress: progressOutput,
	}

	_, _, wN, _, err := snapshot.WriteTo(&appendableWriter{nLog}, nil, wopts)
	if err != nil {
		return err
	}

	err = nLog.Flush()
	if err != nil {
		return err
	}

	err = nLog.Sync()
	if err != nil {
		return err
	}

	// history log is not dumped but to ensure it's fully synced
	err = t.hLog.Sync()
	if err != nil {
		return err
	}

	hLogSize, err := t.hLog.Size()
	if err != nil {
		return err
	}

	rootSize, err := snapshot.root.size()
	if err != nil {
		return err
	}

	// initial and final sizes are set to the same value so to avoid calculating digests of everything
	// it's safe as node and history log files are already synced
	cLogEntry := &cLogEntry{
		initialNLogSize: wN,
		finalNLogSize:   wN,
		rootNodeSize:    rootSize,

		initialHLogSize: hLogSize,
		finalHLogSize:   hLogSize,
	}

	cLogEntry.nLogChecksum, err = appendable.Checksum(nLog, cLogEntry.initialNLogSize, cLogEntry.finalNLogSize-cLogEntry.initialNLogSize)
	if err != nil {
		return err
	}

	cLogEntry.hLogChecksum, err = appendable.Checksum(t.hLog, cLogEntry.initialHLogSize, cLogEntry.finalHLogSize-cLogEntry.initialHLogSize)
	if err != nil {
		return err
	}

	_, _, err = cLog.Append(cLogEntry.serialize())
	if err != nil {
		return err
	}

	err = cLog.Flush()
	if err != nil {
		return err
	}

	err = cLog.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (t *TBtree) Close() error {
	t.logger.Infof("closing index '%s' {ts=%d}...", t.path, t.root.ts())

	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	if len(t.snapshots) > 0 {
		return ErrSnapshotsNotClosed
	}

	t.closed = true

	merrors := multierr.NewMultiErr()

	_, _, err := t.flushTree(0, true, false, "close")
	merrors.Append(err)

	err = t.nLog.Close()
	merrors.Append(err)

	err = t.hLog.Close()
	merrors.Append(err)

	err = t.cLog.Close()
	merrors.Append(err)

	err = merrors.Reduce()
	if err != nil {
		return t.wrapNwarn("closing index '%s' {ts=%d} returned: %v", t.path, t.root.ts(), err)
	}

	t.logger.Infof("index '%s' {ts=%d} successfully closed", t.path, t.root.ts())
	return nil
}

func (t *TBtree) IncreaseTs(ts uint64) error {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return ErrAlreadyClosed
	}

	root, err := t.root.setTs(ts)
	if err != nil {
		return err
	}

	t.root = root

	t.insertionCountSinceFlush++
	t.insertionCountSinceSync++
	t.insertionCountSinceCleanup++

	if t.insertionCountSinceFlush >= t.flushThld {
		_, _, err := t.flushTree(t.cleanupPercentage, false, false, "increaseTs")
		return err
	}
	return nil
}

type KVT struct {
	K []byte
	V []byte
	T uint64
}

func (t *TBtree) lock() {
	t.rwmutex.Lock()
}

func (t *TBtree) unlock() {
	slowDown := t.compacting && t.delayDuringCompaction > 0

	t.rwmutex.Unlock()

	if slowDown {
		time.Sleep(t.delayDuringCompaction)
	}
}

func (t *TBtree) Insert(key []byte, value []byte) error {
	t.lock()
	defer t.unlock()

	return t.bulkInsert([]*KVT{{K: key, V: value}})
}

// BulkInsert inserts multiple entries atomically.
// It is possible to specify a logical timestamp for each entry.
// Timestamps with zero will be associated with the current time plus one.
// The specified timestamp must be greater than the root's current timestamp.
// Timestamps must be increased by one for each additional entry for a key.
func (t *TBtree) BulkInsert(kvts []*KVT) error {
	t.lock()
	defer t.unlock()

	return t.bulkInsert(kvts)
}

func estimateSize(kvts []*KVT) int {
	size := 0
	for _, kv := range kvts {
		size += len(kv.K) + len(kv.V) + 8
	}
	return size
}

func (t *TBtree) bulkInsert(kvts []*KVT) error {
	if t.closed {
		return ErrAlreadyClosed
	}

	if len(kvts) == 0 {
		return ErrIllegalArguments
	}

	entriesSize := estimateSize(kvts)
	if t.bufferedDataSize > 0 && t.bufferedDataSize+entriesSize > t.maxBufferedDataSize {
		_, _, err := t.flushTree(t.cleanupPercentage, false, false, "bulkInsert")
		if err != nil {
			return err
		}
	}
	t.bufferedDataSize += entriesSize

	currTs := t.root.ts()

	// newTs will hold the greatest time, the minimun value will be currTs + 1
	var newTs uint64

	// validated immutable copy of input kv pairs
	immutableKVTs := make([]*KVT, len(kvts))

	for i, kvt := range kvts {
		if kvt == nil || len(kvt.K) == 0 || len(kvt.V) == 0 {
			return ErrIllegalArguments
		}

		if len(kvt.K) > t.maxKeySize {
			return ErrorMaxKeySizeExceeded
		}

		if len(kvt.V) > t.maxValueSize {
			return ErrorMaxValueSizeExceeded
		}

		k := make([]byte, len(kvt.K))
		copy(k, kvt.K)

		v := make([]byte, len(kvt.V))
		copy(v, kvt.V)

		t := kvt.T

		if t == 0 {
			// zero-valued timestamps are associated with current time plus one
			t = currTs + 1
		} else if kvt.T <= currTs { // insertion with a timestamp older or equal to the current timestamp should not be allowed
			return fmt.Errorf("%w: specific timestamp is older than root's current timestamp", ErrIllegalArguments)
		}

		immutableKVTs[i] = &KVT{
			K: k,
			V: v,
			T: t,
		}

		if t > newTs {
			newTs = t
		}
	}

	nodes, depth, err := t.root.insert(immutableKVTs)
	if err != nil {
		// INVARIANT: if !node.mutated() then for every node 'n' in the subtree with node as root !n.mutated() also holds
		// if t.root is not mutated it means no change was made on any node of the tree. Thus no rollback is needed

		if t.root.mutated() {
			// changes may need to be rolled back
			// the most recent snapshot becomes the root again or a fresh start if no snapshots are stored
			if t.lastSnapRoot == nil {
				t.root = &leafNode{t: t, mut: true}
			} else {
				t.root = t.lastSnapRoot
			}
		}

		return err
	}

	for len(nodes) > 1 {
		newRoot := &innerNode{
			t:     t,
			nodes: nodes,
			_ts:   newTs,
			mut:   true,
		}

		depth++

		nodes, err = newRoot.split()
		if err != nil {
			return err
		}
	}

	t.root = nodes[0]

	metricsBtreeDepth.WithLabelValues(t.path).Set(float64(depth))

	t.insertionCountSinceFlush += len(immutableKVTs)
	t.insertionCountSinceSync += len(immutableKVTs)
	t.insertionCountSinceCleanup += len(immutableKVTs)

	if t.insertionCountSinceFlush >= t.flushThld {
		_, _, err := t.flushTree(t.cleanupPercentage, false, false, "bulkInsert")
		return err
	}

	return nil
}

func (t *TBtree) Ts() uint64 {
	t.rwmutex.RLock()
	defer t.rwmutex.RUnlock()

	return t.root.ts()
}

func (t *TBtree) SyncSnapshot() (*Snapshot, error) {
	t.rwmutex.RLock()

	if t.closed {
		return nil, ErrAlreadyClosed
	}

	return &Snapshot{
		id:      math.MaxUint64,
		t:       t,
		ts:      t.root.ts(),
		root:    t.root,
		readers: make(map[int]io.Closer),
		_buf:    make([]byte, t.maxNodeSize),
	}, nil
}

func (t *TBtree) Snapshot() (*Snapshot, error) {
	return t.SnapshotMustIncludeTs(0)
}

func (t *TBtree) SnapshotMustIncludeTs(ts uint64) (*Snapshot, error) {
	return t.SnapshotMustIncludeTsWithRenewalPeriod(ts, t.renewSnapRootAfter)
}

// SnapshotMustIncludeTsWithRenewalPeriod returns a new snapshot based on an existent dumped root (snapshot reuse).
// Current root may be dumped if there are no previous root already stored on disk or if the dumped one was old enough.
// If ts is 0, any snapshot not older than renewalPeriod may be used.
// If renewalPeriod is 0, renewal period is not taken into consideration
func (t *TBtree) SnapshotMustIncludeTsWithRenewalPeriod(ts uint64, renewalPeriod time.Duration) (*Snapshot, error) {
	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	if t.closed {
		return nil, ErrAlreadyClosed
	}

	if ts > t.root.ts() {
		return nil, fmt.Errorf("%w: ts is greater than current ts", ErrIllegalArguments)
	}

	if len(t.snapshots) == t.maxActiveSnapshots {
		return nil, ErrorToManyActiveSnapshots
	}

	// the tbtree will be flushed if the current root is mutated, the data on disk is not synchronized,
	// and no snapshot on disk can be re-used.
	if t.root.mutated() {
		// it means the current root is not stored on disk

		var snapshotRenewalNeeded bool

		if t.lastSnapRoot == nil {
			snapshotRenewalNeeded = true
		} else if t.lastSnapRoot.ts() < t.root.ts() {
			snapshotRenewalNeeded = t.lastSnapRoot.ts() < ts ||
				(renewalPeriod > 0 && time.Since(t.lastSnapRootAt) >= renewalPeriod)
		}

		if snapshotRenewalNeeded {
			// a new snapshot is dumped on disk including current root
			_, _, err := t.flushTree(t.cleanupPercentage, false, false, "snapshotSince")
			if err != nil {
				return nil, err
			}
			// !t.root.mutated() hold as this point
		}
	}

	if !t.root.mutated() {
		// either if the root was not updated or if it was dumped as part of a snapshot renewal
		t.lastSnapRoot = t.root
		t.lastSnapRootAt = time.Now()
	}

	t.maxSnapshotID++

	snapshot := t.newSnapshot(t.maxSnapshotID, t.lastSnapRoot)

	t.snapshots[snapshot.id] = snapshot

	return snapshot, nil
}

func (t *TBtree) newSnapshot(snapshotID uint64, root node) *Snapshot {
	return &Snapshot{
		t:       t,
		id:      snapshotID,
		ts:      root.ts() + 1,
		root:    root,
		readers: make(map[int]io.Closer),
		_buf:    make([]byte, t.maxNodeSize),
	}
}

func (t *TBtree) snapshotClosed(snapshot *Snapshot) error {
	if snapshot.id == math.MaxUint64 {
		t.rwmutex.RUnlock()
		return nil
	}

	t.rwmutex.Lock()
	defer t.rwmutex.Unlock()

	delete(t.snapshots, snapshot.id)

	return nil
}

func (n *innerNode) insert(kvts []*KVT) (nodes []node, depth int, err error) {
	if n.mutated() {
		return n.updateOnInsert(kvts)
	}

	newNode := &innerNode{
		t:       n.t,
		nodes:   make([]node, len(n.nodes)),
		_ts:     n._ts,
		_minOff: n._minOff,
		mut:     true,
	}

	copy(newNode.nodes, n.nodes)

	return newNode.updateOnInsert(kvts)
}

func (n *innerNode) updateOnInsert(kvts []*KVT) (nodes []node, depth int, err error) {
	// group kvs by child at which they will be inserted
	kvtsPerChild := make(map[int][]*KVT)

	for _, kvt := range kvts {
		childIndex := n.indexOf(kvt.K)
		kvtsPerChild[childIndex] = append(kvtsPerChild[childIndex], kvt)
	}

	var wg sync.WaitGroup
	wg.Add(len(kvtsPerChild))

	nodesPerChild := make(map[int][]node)
	var nodesMutex sync.Mutex

	for childIndex, childKVTs := range kvtsPerChild {
		// insert kvs at every child simultaneously
		go func(childIndex int, childKVTs []*KVT) {
			defer wg.Done()

			child := n.nodes[childIndex]

			newChildren, childrenDepth, childrenErr := child.insert(childKVTs)

			nodesMutex.Lock()
			defer nodesMutex.Unlock()

			if childrenErr != nil {
				// if any of its children fail to insert, insertion fails
				err = childrenErr
				return
			}

			nodesPerChild[childIndex] = newChildren
			if childrenDepth > depth {
				depth = childrenDepth
			}

			for _, newChild := range newChildren {
				if newChild.ts() > n._ts {
					n._ts = newChild.ts()
				}
			}

		}(childIndex, childKVTs)
	}

	// wait for all the insertions to be done
	wg.Wait()

	if err != nil {
		return nil, 0, err
	}

	// count the number of children after insertion
	nsSize := len(n.nodes)

	for i := range n.nodes {
		cs, ok := nodesPerChild[i]
		if ok {
			nsSize += len(cs) - 1
		}
	}

	ns := make([]node, nsSize)
	nsi := 0

	for i, n := range n.nodes {
		cs, ok := nodesPerChild[i]
		if ok {
			copy(ns[nsi:], cs)
			nsi += len(cs)
		} else {
			ns[nsi] = n
			nsi++
		}
	}

	n.nodes = ns

	nodes, err = n.split()
	if err != nil {
		return nil, 0, err
	}

	return nodes, depth + 1, nil
}

func (n *innerNode) get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	return n.nodes[n.indexOf(key)].get(key)
}

func (n *innerNode) getBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	return n.nodes[n.indexOf(key)].getBetween(key, initialTs, finalTs)
}

func (n *innerNode) history(key []byte, offset uint64, descOrder bool, limit int) ([]TimedValue, uint64, error) {
	return n.nodes[n.indexOf(key)].history(key, offset, descOrder, limit)
}

func (n *innerNode) findLeafNode(seekKey []byte, path path, offset int, neqKey []byte, descOrder bool) (path, *leafNode, int, error) {
	metricsBtreeInnerNodeEntries.WithLabelValues(n.t.path).Observe(float64(len(n.nodes)))

	if descOrder {
		for i := offset; i < len(n.nodes); i++ {
			j := len(n.nodes) - 1 - i
			minKey := n.nodes[j].minKey()

			if len(neqKey) > 0 && bytes.Compare(minKey, neqKey) >= 0 {
				continue
			}

			if bytes.Compare(minKey, seekKey) < 1 {
				return n.nodes[j].findLeafNode(seekKey, append(path, &pathNode{node: n, offset: i}), 0, neqKey, descOrder)
			}
		}

		return nil, nil, 0, ErrKeyNotFound
	}

	if offset > len(n.nodes)-1 {
		return nil, nil, 0, ErrKeyNotFound
	}

	for i := offset; i < len(n.nodes)-1; i++ {
		nextMinKey := n.nodes[i+1].minKey()

		if bytes.Compare(seekKey, nextMinKey) >= 0 {
			continue
		}

		if len(neqKey) > 0 && bytes.Compare(nextMinKey, neqKey) < 0 {
			continue
		}

		path, leafNode, off, err := n.nodes[i].findLeafNode(seekKey, append(path, &pathNode{node: n, offset: i}), 0, neqKey, descOrder)
		if errors.Is(err, ErrKeyNotFound) {
			continue
		}

		return path, leafNode, off, err
	}

	return n.nodes[len(n.nodes)-1].findLeafNode(seekKey, append(path, &pathNode{node: n, offset: len(n.nodes) - 1}), 0, neqKey, descOrder)
}

func (n *innerNode) ts() uint64 {
	return n._ts
}

func (n *innerNode) setTs(ts uint64) (node, error) {
	if n._ts >= ts {
		return nil, ErrIllegalArguments
	}

	if n.mut {
		n._ts = ts
		return n, nil
	}

	newNode := &innerNode{
		t:     n.t,
		nodes: make([]node, len(n.nodes)),
		_ts:   ts,
		mut:   true,
	}

	copy(newNode.nodes, n.nodes)

	return newNode, nil
}

// size calculates the amount of bytes required to serialize an inner node
// note: requiredNodeSize must be revised if this function is modified
func (n *innerNode) size() (int, error) {
	size := 1 // Node type

	size += 2 // Child count

	for _, c := range n.nodes {
		size += 2               // minKey length
		size += len(c.minKey()) // minKey
		size += 8               // ts
		size += 8               // offset
		size += 8               // min offset
	}

	return size, nil
}

func (l *innerNode) tsMutated() bool {
	for _, nd := range l.nodes {
		if nd.ts() >= l.ts() {
			return false
		}
	}
	return true
}

func (n *innerNode) mutated() bool {
	return n.mut
}

func (n *innerNode) offset() int64 {
	return n.off
}

func (n *innerNode) minOffset() int64 {
	return n._minOff
}

func (n *innerNode) minKey() []byte {
	if len(n.nodes) == 0 {
		return nil
	}
	return n.nodes[0].minKey()
}

// indexOf returns the first child at which key is equal or greater than its minKey
func (n *innerNode) indexOf(key []byte) int {
	metricsBtreeInnerNodeEntries.WithLabelValues(n.t.path).Observe(float64(len(n.nodes)))

	left := 0
	right := len(n.nodes) - 1

	var middle int
	var diff int

	for left < right {
		middle = left + (right-left)/2 + 1

		minKey := n.nodes[middle].minKey()

		diff = bytes.Compare(minKey, key)

		if diff == 0 {
			return middle
		} else if diff < 0 {
			// minKey < key
			left = middle
		} else {
			// minKey > key
			right = middle - 1
		}
	}

	return left
}

func (n *innerNode) split() ([]node, error) {
	size, err := n.size()
	if err != nil {
		return nil, err
	}

	if size <= n.t.maxNodeSize {
		metricsBtreeInnerNodeEntries.WithLabelValues(n.t.path).Observe(float64(len(n.nodes)))
		return []node{n}, nil
	}

	splitIndex := splitIndex(len(n.nodes))

	newNode := &innerNode{
		t:     n.t,
		nodes: n.nodes[splitIndex:],
		mut:   true,
	}
	newNode.updateTs()

	n.nodes = n.nodes[:splitIndex]
	n.updateTs()

	ns1, err := n.split()
	if err != nil {
		return nil, err
	}

	ns2, err := newNode.split()
	if err != nil {
		return nil, err
	}

	return append(ns1, ns2...), nil
}

func (n *innerNode) updateTs() {
	n._ts = 0

	for i := 0; i < len(n.nodes); i++ {
		if n.ts() < n.nodes[i].ts() {
			n._ts = n.nodes[i].ts()
		}
	}
}

////////////////////////////////////////////////////////////

func (r *nodeRef) insert(kvts []*KVT) (nodes []node, depth int, err error) {
	n, err := r.t.nodeAt(r.off, true)
	if err != nil {
		return nil, 0, err
	}
	return n.insert(kvts)
}

func (r *nodeRef) get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	n, err := r.t.nodeAt(r.off, true)
	if err != nil {
		return nil, 0, 0, err
	}
	return n.get(key)
}

func (r *nodeRef) getBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	n, err := r.t.nodeAt(r.off, true)
	if err != nil {
		return nil, 0, 0, err
	}
	return n.getBetween(key, initialTs, finalTs)
}

func (r *nodeRef) history(key []byte, offset uint64, descOrder bool, limit int) ([]TimedValue, uint64, error) {
	n, err := r.t.nodeAt(r.off, true)
	if err != nil {
		return nil, 0, err
	}
	return n.history(key, offset, descOrder, limit)
}

func (r *nodeRef) findLeafNode(seekKey []byte, path path, offset int, neqKey []byte, descOrder bool) (path, *leafNode, int, error) {
	n, err := r.t.nodeAt(r.off, true)
	if err != nil {
		return nil, nil, 0, err
	}
	return n.findLeafNode(seekKey, path, offset, neqKey, descOrder)
}

func (r *nodeRef) minKey() []byte {
	return r._minKey
}

func (r *nodeRef) ts() uint64 {
	return r._ts
}

func (r *nodeRef) minOffset() int64 {
	return r._minOff
}

func (r *nodeRef) setTs(ts uint64) (node, error) {
	n, err := r.t.nodeAt(r.off, false)
	if err != nil {
		return nil, err
	}

	return n.setTs(ts)
}

func (r *nodeRef) size() (int, error) {
	n, err := r.t.nodeAt(r.off, false)
	if err != nil {
		return 0, err
	}

	return n.size()
}

func (r *nodeRef) tsMutated() bool {
	return false
}

func (r *nodeRef) mutated() bool {
	return false
}

func (r *nodeRef) offset() int64 {
	return r.off
}

////////////////////////////////////////////////////////////

func (l *leafNode) insert(kvts []*KVT) (nodes []node, depth int, err error) {
	if l.mutated() {
		return l.updateOnInsert(kvts)
	}

	newLeaf := &leafNode{
		t:      l.t,
		values: make([]*leafValue, len(l.values)),
		_ts:    l._ts,
		mut:    true,
	}

	for i, lv := range l.values {
		timedValues := make([]TimedValue, len(lv.timedValues))
		copy(timedValues, lv.timedValues)

		newLeaf.values[i] = &leafValue{
			key:         lv.key,
			timedValues: timedValues,
			hOff:        lv.hOff,
			hCount:      lv.hCount,
		}
	}

	return newLeaf.updateOnInsert(kvts)
}

func (l *leafNode) updateOnInsert(kvts []*KVT) (nodes []node, depth int, err error) {
	for _, kvt := range kvts {
		i, found := l.indexOf(kvt.K)

		if found {
			lv := l.values[i]

			if kvt.T < lv.timedValue().Ts {
				// The validation can be done upfront at bulkInsert,
				// but postponing it could reduce resource requirements during the earlier stages,
				// resulting in higher performance due to concurrency.
				return nil, 0, fmt.Errorf("%w: attempt to insert a value without an older timestamp", ErrIllegalArguments)
			}

			if kvt.T > lv.timedValue().Ts {
				lv.timedValues = append([]TimedValue{{Value: kvt.V, Ts: kvt.T}}, lv.timedValues...)
			}
		} else {
			values := make([]*leafValue, len(l.values)+1)

			copy(values, l.values[:i])

			values[i] = &leafValue{
				key:         kvt.K,
				timedValues: []TimedValue{{Value: kvt.V, Ts: kvt.T}},
			}

			copy(values[i+1:], l.values[i:])

			l.values = values
		}

		if l._ts < kvt.T {
			l._ts = kvt.T
		}
	}

	nodes, err = l.split()

	return nodes, 1, err
}

func (l *leafNode) get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	i, found := l.indexOf(key)

	if !found {
		return nil, 0, 0, ErrKeyNotFound
	}

	leafValue := l.values[i]
	timedValue := leafValue.timedValue()

	return timedValue.Value, timedValue.Ts, leafValue.historyCount(), nil
}

func (l *leafNode) getBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	i, found := l.indexOf(key)

	if !found {
		return nil, 0, 0, ErrKeyNotFound
	}

	leafValue := l.values[i]

	return leafValue.lastUpdateBetween(l.t.hLog, initialTs, finalTs)
}

func (l *leafNode) history(key []byte, offset uint64, desc bool, limit int) ([]TimedValue, uint64, error) {
	i, found := l.indexOf(key)

	if !found {
		return nil, 0, ErrKeyNotFound
	}

	leafValue := l.values[i]

	return leafValue.history(key, offset, desc, limit, l.t.hLog)
}

func (lv *leafValue) history(key []byte, offset uint64, desc bool, limit int, hLog appendable.Appendable) ([]TimedValue, uint64, error) {
	hCount := lv.historyCount()

	if offset == hCount {
		return nil, 0, ErrNoMoreEntries
	}

	if offset > hCount {
		return nil, 0, ErrOffsetOutOfRange
	}

	timedValuesLen := limit
	if uint64(limit) > hCount-offset {
		timedValuesLen = int(hCount - offset)
	}

	timedValues := make([]TimedValue, timedValuesLen)

	initAt := offset
	tssOff := 0

	if !desc {
		initAt = hCount - offset - uint64(timedValuesLen)
	}

	if initAt < uint64(len(lv.timedValues)) {
		for i := int(initAt); i < len(lv.timedValues) && tssOff < timedValuesLen; i++ {
			if desc {
				timedValues[tssOff] = lv.timedValues[i]
			} else {
				timedValues[timedValuesLen-1-tssOff] = lv.timedValues[i]
			}

			tssOff++
		}
	}

	hOff := lv.hOff

	ti := uint64(len(lv.timedValues))

	for tssOff < timedValuesLen {
		r := appendable.NewReaderFrom(hLog, hOff, DefaultMaxNodeSize)

		hc, err := r.ReadUint32()
		if err != nil {
			return nil, 0, err
		}

		for i := 0; i < int(hc) && tssOff < timedValuesLen; i++ {
			valueLen, err := r.ReadUint16()
			if err != nil {
				return nil, 0, err
			}

			value := make([]byte, valueLen)
			_, err = r.Read(value)
			if err != nil {
				return nil, 0, err
			}

			ts, err := r.ReadUint64()
			if err != nil {
				return nil, 0, err
			}

			if ti < initAt {
				ti++
				continue
			}

			if desc {
				timedValues[tssOff] = TimedValue{Value: value, Ts: ts}
			} else {
				timedValues[timedValuesLen-1-tssOff] = TimedValue{Value: value, Ts: ts}
			}

			tssOff++
		}

		prevOff, err := r.ReadUint64()
		if err != nil {
			return nil, 0, err
		}

		hOff = int64(prevOff)
	}

	return timedValues, hCount, nil
}

func (l *leafNode) findLeafNode(seekKey []byte, path path, _ int, neqKey []byte, descOrder bool) (path, *leafNode, int, error) {
	metricsBtreeLeafNodeEntries.WithLabelValues(l.t.path).Observe(float64(len(l.values)))
	if descOrder {
		for i := len(l.values); i > 0; i-- {
			key := l.values[i-1].key

			if len(neqKey) > 0 && bytes.Compare(key, neqKey) >= 0 {
				continue
			}

			if bytes.Compare(key, seekKey) < 1 {
				return path, l, i - 1, nil
			}
		}

		return nil, nil, 0, ErrKeyNotFound
	}

	for i, v := range l.values {
		if len(neqKey) > 0 && bytes.Compare(v.key, neqKey) <= 0 {
			continue
		}

		if bytes.Compare(seekKey, v.key) < 1 {
			return path, l, i, nil
		}
	}

	return nil, nil, 0, ErrKeyNotFound
}

func (l *leafNode) indexOf(key []byte) (index int, found bool) {
	metricsBtreeLeafNodeEntries.WithLabelValues(l.t.path).Observe(float64(len(l.values)))

	left := 0
	right := len(l.values)

	var middle int
	var diff int

	for left < right {
		middle = left + (right-left)/2

		diff = bytes.Compare(l.values[middle].key, key)

		if diff == 0 {
			return middle, true
		} else if diff < 0 {
			left = middle + 1
		} else {
			right = middle
		}
	}

	return left, false
}

func (l *leafNode) minKey() []byte {
	if len(l.values) == 0 {
		return nil
	}
	return l.values[0].key
}

func (l *leafNode) ts() uint64 {
	return l._ts
}

func (l *leafNode) minOffset() int64 {
	return l.off
}

func (l *leafNode) setTs(ts uint64) (node, error) {
	if l._ts >= ts {
		return nil, ErrIllegalArguments
	}

	if l.mut {
		l._ts = ts
		return l, nil
	}

	newLeaf := &leafNode{
		t:      l.t,
		values: make([]*leafValue, len(l.values)),
		_ts:    ts,
		mut:    true,
	}

	for i := 0; i < len(l.values); i++ {
		lv := l.values[i]

		timedValues := make([]TimedValue, len(lv.timedValues))
		copy(timedValues, lv.timedValues)

		newLeaf.values[i] = &leafValue{
			key:         lv.key,
			timedValues: timedValues,
			hOff:        lv.hOff,
			hCount:      lv.hCount,
		}
	}

	return newLeaf, nil
}

// size calculates the amount of bytes required to serialize a leaf node
// note: requiredNodeSize must be revised if this function is modified
func (l *leafNode) size() (int, error) {
	size := 1 // Node type

	size += 2 // kv count

	for _, kv := range l.values {
		tv := kv.timedValue()

		size += 2             // Key length
		size += len(kv.key)   // Key
		size += 2             // Value length
		size += len(tv.Value) // Value
		size += 8             // Ts
		size += 8             // hOff
		size += 8             // hCount
	}

	return size, nil
}

func (l *leafNode) tsMutated() bool {
	for _, v := range l.values {
		if v.timedValues[0].Ts >= l.ts() {
			return false
		}
	}
	return true
}

func (l *leafNode) mutated() bool {
	return l.mut
}

func (l *leafNode) offset() int64 {
	return l.off
}

func (l *leafNode) split() ([]node, error) {
	size, err := l.size()
	if err != nil {
		return nil, err
	}

	if size <= l.t.maxNodeSize {
		metricsBtreeLeafNodeEntries.WithLabelValues(l.t.path).Observe(float64(len(l.values)))
		return []node{l}, nil
	}

	splitIndex := splitIndex(len(l.values))

	newLeaf := &leafNode{
		t:      l.t,
		values: l.values[splitIndex:],
		mut:    true,
	}
	newLeaf.updateTs()

	l.values = l.values[:splitIndex]
	l.updateTs()

	ns1, err := l.split()
	if err != nil {
		return nil, err
	}

	ns2, err := newLeaf.split()
	if err != nil {
		return nil, err
	}

	return append(ns1, ns2...), nil
}

func splitIndex(sz int) int {
	if sz%2 == 0 {
		return sz / 2
	}
	return sz/2 + 1
}

func (l *leafNode) updateTs() {
	l._ts = 0

	for i := 0; i < len(l.values); i++ {
		if l._ts < l.values[i].timedValue().Ts {
			l._ts = l.values[i].timedValue().Ts
		}
	}
}

func (lv *leafValue) timedValue() TimedValue {
	return lv.timedValues[0]
}

func (lv *leafValue) historyCount() uint64 {
	return lv.hCount + uint64(len(lv.timedValues))
}

func (lv *leafValue) size() int {
	return 16 + len(lv.key) + len(lv.timedValue().Value)
}

func (lv *leafValue) lastUpdateBetween(hLog appendable.Appendable, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	if initialTs > finalTs {
		return nil, 0, 0, ErrIllegalArguments
	}

	for i, tv := range lv.timedValues {
		if tv.Ts < initialTs {
			return nil, 0, 0, ErrKeyNotFound
		}

		if finalTs == 0 || tv.Ts <= finalTs {
			return tv.Value, tv.Ts, lv.historyCount() - uint64(i), nil
		}
	}

	hOff := lv.hOff
	skippedUpdates := uint64(0)

	for i := uint64(0); i < lv.hCount; i++ {
		r := appendable.NewReaderFrom(hLog, hOff, DefaultMaxNodeSize)

		hc, err := r.ReadUint32()
		if err != nil {
			return nil, 0, 0, err
		}

		for j := 0; j < int(hc); j++ {
			valueLen, err := r.ReadUint16()
			if err != nil {
				return nil, 0, 0, err
			}

			value := make([]byte, valueLen)
			_, err = r.Read(value)
			if err != nil {
				return nil, 0, 0, err
			}

			ts, err := r.ReadUint64()
			if err != nil {
				return nil, 0, 0, err
			}

			if ts < initialTs {
				return nil, 0, 0, ErrKeyNotFound
			}

			if ts <= finalTs {
				return value, ts, lv.hCount - skippedUpdates, nil
			}

			skippedUpdates++
		}

		prevOff, err := r.ReadUint64()
		if err != nil {
			return nil, 0, 0, err
		}

		hOff = int64(prevOff)
	}

	return nil, 0, 0, ErrKeyNotFound
}
