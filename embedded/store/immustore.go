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

package store

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded"
	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/appendable/singleapp"
	"github.com/codenotary/immudb/embedded/cache"
	"github.com/codenotary/immudb/embedded/htree"
	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/embedded/watchers"
	"github.com/codenotary/immudb/pkg/helpers/semaphore"
	"github.com/codenotary/immudb/pkg/helpers/slices"
)

var ErrIllegalArguments = embedded.ErrIllegalArguments
var ErrInvalidOptions = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
var ErrAlreadyClosed = embedded.ErrAlreadyClosed
var ErrUnexpectedLinkingError = errors.New("internal inconsistency between linear and binary linking")
var ErrNoEntriesProvided = errors.New("no entries provided")
var ErrWriteOnlyTx = errors.New("write-only transaction")
var ErrReadOnlyTx = errors.New("read-only transaction")
var ErrTxReadConflict = errors.New("tx read conflict")
var ErrTxAlreadyCommitted = errors.New("tx already committed")
var ErrMaxTxEntriesLimitExceeded = errors.New("max number of entries per tx exceeded")
var ErrNullKey = errors.New("null key")
var ErrMaxKeyLenExceeded = errors.New("max key length exceeded")
var ErrMaxValueLenExceeded = errors.New("max value length exceeded")
var ErrPreconditionFailed = errors.New("precondition failed")
var ErrDuplicatedKey = errors.New("duplicated key")
var ErrCannotUpdateKeyTransiency = errors.New("cannot change a non-transient key to transient or vice versa")
var ErrMaxActiveTransactionsLimitExceeded = errors.New("max active transactions limit exceeded")
var ErrMVCCReadSetLimitExceeded = errors.New("MVCC read-set limit exceeded")
var ErrMaxConcurrencyLimitExceeded = errors.New("max concurrency limit exceeded")
var ErrMaxIndexersLimitExceeded = errors.New("max indexers limit exceeded")
var ErrPathIsNotADirectory = errors.New("path is not a directory")
var ErrCorruptedTxData = errors.New("tx data is corrupted")
var ErrCorruptedTxDataMaxTxEntriesExceeded = fmt.Errorf("%w: maximum number of TX entries exceeded", ErrCorruptedTxData)
var ErrTxEntryIndexOutOfRange = errors.New("tx entry index out of range")
var ErrCorruptedTxDataUnknownHeaderVersion = fmt.Errorf("%w: unknown TX header version", ErrCorruptedTxData)
var ErrCorruptedTxDataMaxKeyLenExceeded = fmt.Errorf("%w: maximum key length exceeded", ErrCorruptedTxData)
var ErrCorruptedTxDataDuplicateKey = fmt.Errorf("%w: duplicate key in a single TX", ErrCorruptedTxData)
var ErrCorruptedData = errors.New("data is corrupted")
var ErrCorruptedCLog = errors.New("commit-log is corrupted")
var ErrCorruptedIndex = errors.New("corrupted index")
var ErrTxSizeGreaterThanMaxTxSize = errors.New("tx size greater than max tx size")
var ErrCorruptedAHtree = errors.New("appendable hash tree is corrupted")
var ErrKeyNotFound = tbtree.ErrKeyNotFound // TODO: define error in store layer
var ErrExpiredEntry = fmt.Errorf("%w: expired entry", ErrKeyNotFound)
var ErrKeyAlreadyExists = errors.New("key already exists")
var ErrTxNotFound = errors.New("tx not found")
var ErrNoMoreEntries = tbtree.ErrNoMoreEntries       // TODO: define error in store layer
var ErrIllegalState = tbtree.ErrIllegalState         // TODO: define error in store layer
var ErrOffsetOutOfRange = tbtree.ErrOffsetOutOfRange // TODO: define error in store layer
var ErrUnexpectedError = errors.New("unexpected error")
var ErrUnsupportedTxVersion = errors.New("unsupported tx version")
var ErrNewerVersionOrCorruptedData = errors.New("tx created with a newer version or data is corrupted")
var ErrTxPoolExhausted = errors.New("transaction pool exhausted")

var ErrInvalidPrecondition = errors.New("invalid precondition")
var ErrInvalidPreconditionTooMany = fmt.Errorf("%w: too many preconditions", ErrInvalidPrecondition)
var ErrInvalidPreconditionNull = fmt.Errorf("%w: null", ErrInvalidPrecondition)
var ErrInvalidPreconditionNullKey = fmt.Errorf("%w: %v", ErrInvalidPrecondition, ErrNullKey)
var ErrInvalidPreconditionMaxKeyLenExceeded = fmt.Errorf("%w: %v", ErrInvalidPrecondition, ErrMaxKeyLenExceeded)
var ErrInvalidPreconditionInvalidTxID = fmt.Errorf("%w: invalid transaction ID", ErrInvalidPrecondition)

var ErrSourceTxNewerThanTargetTx = fmt.Errorf("%w: source tx is newer than target tx", ErrIllegalArguments)

var ErrCompactionDisabled = errors.New("compaction is disabled")

var ErrMetadataUnsupported = errors.New(
	"metadata is unsupported when in 1.1 compatibility mode, " +
		"do not use metadata-related features such as expiration and logical deletion",
)

var ErrUnsupportedTxHeaderVersion = errors.New("missing tx header serialization method")
var ErrIllegalTruncationArgument = fmt.Errorf("%w: invalid truncation info", ErrIllegalArguments)
var ErrTruncationInfoNotPresentInMetadata = errors.New("truncation info not present in metadata")

var ErrInvalidProof = errors.New("invalid proof")

var ErrIndexNotFound = errors.New("index not found")
var ErrIndexAlreadyInitialized = errors.New("index already initialized")

const MaxKeyLen = 1024 // assumed to be not lower than hash size
const MaxParallelIO = 127

const cLogEntrySizeV1 = offsetSize + lszSize               // tx offset + hdr size
const cLogEntrySizeV2 = offsetSize + lszSize + sha256.Size // tx offset + hdr size + alh

const txIDSize = 8
const tsSize = 8
const lszSize = 4
const sszSize = 2
const offsetSize = 8

// Version 2 includes `metaEmbeddedValues` and `metaPreallocFiles` into clog metadata
const Version = 2

const MaxTxHeaderVersion = 1

const (
	metaVersion        = "VERSION"
	metaMaxTxEntries   = "MAX_TX_ENTRIES"
	metaMaxKeyLen      = "MAX_KEY_LEN"
	metaMaxValueLen    = "MAX_VALUE_LEN"
	metaFileSize       = "FILE_SIZE"
	metaEmbeddedValues = "EMBEDDED_VALUES"
	metaPreallocFiles  = "PREALLOC_FILES"
)

const indexDirname = "index"
const ahtDirname = "aht"

type ImmuStore struct {
	path string

	logger           logger.Logger
	lastNotification time.Time
	notifyMutex      sync.Mutex

	vLogs            map[byte]*refVLog
	vLogUnlockedList *list.List
	vLogsCond        *sync.Cond

	vLogCache *cache.Cache

	txLog      appendable.Appendable
	txLogCache *cache.Cache

	cLog          appendable.Appendable
	cLogEntrySize int

	cLogBuf *precommitBuffer

	committedTxID uint64
	committedAlh  [sha256.Size]byte

	inmemPrecommittedTxID uint64
	inmemPrecommittedAlh  [sha256.Size]byte

	precommittedTxLogSize int64

	mandatoryMVCCUpToTxID uint64

	commitStateRWMutex sync.RWMutex

	embeddedValues        bool
	preallocFiles         bool
	readOnly              bool
	synced                bool
	syncFrequency         time.Duration
	maxActiveTransactions int
	mvccReadSetLimit      int
	maxWaitees            int
	maxConcurrency        int
	maxIOConcurrency      int
	maxTxEntries          int
	maxKeyLen             int
	maxValueLen           int

	writeTxHeaderVersion int

	timeFunc      TimeFunc
	multiIndexing bool

	useExternalCommitAllowance bool
	commitAllowedUpToTxID      uint64

	txPool TxPool

	waiteesMutex sync.Mutex
	waiteesCount int // current number of go-routines waiting for a tx to be indexed or committed

	_txbs     []byte                   // pre-allocated buffer to support tx serialization
	_valBs    [DefaultMaxValueLen]byte // pre-allocated buffer to support tx exportation
	_valBsMux sync.Mutex

	aht                  *ahtree.AHtree
	inmemPrecommitWHub   *watchers.WatchersHub
	durablePrecommitWHub *watchers.WatchersHub
	commitWHub           *watchers.WatchersHub

	indexers      map[[sha256.Size]byte]*indexer
	nextIndexerID uint64
	indexCache    *cache.Cache

	memSemaphore *semaphore.Semaphore // used by indexers to control amount acquired of memory
	indexersMux  sync.RWMutex

	opts *Options

	closed bool

	mutex sync.Mutex

	compactionDisabled bool
}

type refVLog struct {
	vLog        appendable.Appendable
	unlockedRef *list.Element // unlockedRef == nil <-> vLog is locked
}

func Open(path string, opts *Options) (*ImmuStore, error) {
	err := opts.Validate()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrIllegalArguments, err)
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		err := os.Mkdir(path, opts.FileMode)
		if err != nil {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrPathIsNotADirectory
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(metaVersion, Version)
	metadata.PutBool(metaEmbeddedValues, opts.EmbeddedValues)
	metadata.PutBool(metaPreallocFiles, opts.PreallocFiles)
	metadata.PutInt(metaMaxTxEntries, opts.MaxTxEntries)
	metadata.PutInt(metaMaxKeyLen, opts.MaxKeyLen)
	metadata.PutInt(metaMaxValueLen, opts.MaxValueLen)
	metadata.PutInt(metaFileSize, opts.FileSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithWriteBufferSize(opts.WriteBufferSize).
		WithRetryableSync(opts.Synced).
		WithAutoSync(true).
		WithFileSize(opts.FileSize).
		WithFileMode(opts.FileMode).
		WithMetadata(metadata.Bytes())

	appFactory := opts.appFactory
	if appFactory == nil {
		appFactory = func(rootPath, subPath string, opts *multiapp.Options) (appendable.Appendable, error) {
			path := filepath.Join(rootPath, subPath)
			return multiapp.Open(path, opts)
		}
	}

	appendableOpts.WithFileExt("tx")
	appendableOpts.WithPrealloc(opts.PreallocFiles)
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.TxLogMaxOpenedFiles)
	txLog, err := appFactory(path, "tx", appendableOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to open transaction log: %w", err)
	}

	metadata = appendable.NewMetadata(txLog.Metadata())

	embeddedValues, ok := metadata.GetBool(metaEmbeddedValues)
	embeddedValues = ok && embeddedValues

	preallocFiles, ok := metadata.GetBool(metaPreallocFiles)
	preallocFiles = ok && preallocFiles

	appendableOpts.WithFileExt("txi")
	appendableOpts.WithFileSize(opts.FileSize)
	appendableOpts.WithPrealloc(preallocFiles)
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.CommitLogMaxOpenedFiles)
	cLog, err := appFactory(path, "commit", appendableOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to open commit-log: %w", err)
	}

	var vLogs []appendable.Appendable

	if !embeddedValues {
		vLogs = make([]appendable.Appendable, opts.MaxIOConcurrency)
		appendableOpts.WithFileExt("val")
		appendableOpts.WithFileSize(opts.FileSize)
		appendableOpts.WithPrealloc(false)
		appendableOpts.WithCompressionFormat(opts.CompressionFormat)
		appendableOpts.WithCompresionLevel(opts.CompressionLevel)
		appendableOpts.WithMaxOpenedFiles(opts.VLogMaxOpenedFiles)

		for i := 0; i < opts.MaxIOConcurrency; i++ {
			vLog, err := appFactory(path, fmt.Sprintf("val_%d", i), appendableOpts)
			if err != nil {
				return nil, err
			}
			vLogs[i] = vLog
		}
	}

	return OpenWith(path, vLogs, txLog, cLog, opts)
}

func OpenWith(path string, vLogs []appendable.Appendable, txLog, cLog appendable.Appendable, opts *Options) (*ImmuStore, error) {
	if txLog == nil || cLog == nil {
		return nil, fmt.Errorf("%w: invalid txLog or cLog", ErrIllegalArguments)
	}

	err := opts.Validate()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrIllegalArguments, err)
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	version, ok := metadata.GetInt(metaVersion)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "Version")
	}

	var cLogEntrySize int

	if version <= 1 {
		cLogEntrySize = cLogEntrySizeV1
	} else {
		cLogEntrySize = cLogEntrySizeV2
	}

	embeddedValues, ok := metadata.GetBool(metaEmbeddedValues)
	if !ok {
		if version >= 2 {
			return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "EmbeddedValues")
		}

		embeddedValues = false
	}

	preallocFiles, ok := metadata.GetBool(metaPreallocFiles)
	if !ok {
		if version >= 2 {
			return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "PreallocFiles")
		}

		preallocFiles = false
	}

	if (len(vLogs) == 0 && !embeddedValues) || (len(vLogs) != 0 && embeddedValues) {
		return nil, fmt.Errorf("%w: invalid vLogs", ErrIllegalArguments)
	}

	fileSize, ok := metadata.GetInt(metaFileSize)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "FileSize")
	}

	maxTxEntries, ok := metadata.GetInt(metaMaxTxEntries)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "MaxTxEntries")
	}

	maxKeyLen, ok := metadata.GetInt(metaMaxKeyLen)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "MaxKeyLen")
	}

	maxValueLen, ok := metadata.GetInt(metaMaxValueLen)
	if !ok {
		return nil, fmt.Errorf("%w: can not read '%s' from metadata", ErrCorruptedCLog, "MaxValueLen")
	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, fmt.Errorf("corrupted commit-log: could not get size: %w", err)
	}

	if !preallocFiles {
		rem := cLogSize % int64(cLogEntrySize)
		if rem > 0 {
			cLogSize -= rem
			err = cLog.SetOffset(cLogSize)
			if err != nil {
				return nil, fmt.Errorf("corrupted commit log: could not set offset: %w", err)
			}
		}
	}

	if preallocFiles {
		if cLogSize == 0 {
			return nil, fmt.Errorf("corrupted commit log: file should not be empty when file preallocation is enabled")
		}

		// find the last non-zeroed clogEntry
		left := int64(1)
		right := cLogSize / int64(cLogEntrySize)

		b := make([]byte, cLogEntrySize)
		zeroed := make([]byte, cLogEntrySize)

		for left < right {
			middle := left + ((right-left)+1)/2

			_, err := cLog.ReadAt(b, (middle-1)*int64(cLogEntrySize))
			if err != nil {
				return nil, fmt.Errorf("corrupted commit log: could not read the last commit: %w", err)
			}

			if bytes.Equal(b, zeroed) {
				// if cLogEntry is zeroed it's considered as preallocated
				right = middle - 1
			} else {
				left = middle
			}
		}

		_, err := cLog.ReadAt(b, (left-1)*int64(cLogEntrySize))
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("corrupted commit log: could not read the last commit: %w", err)
		}

		if bytes.Equal(b, zeroed) {
			cLogSize = 0
		} else {
			cLogSize = left * int64(cLogEntrySize)
		}
	}

	var committedTxLogSize int64
	var committedTxOffset int64
	var committedTxSize int

	var committedTxID uint64

	committedAlh := sha256.Sum256(nil)

	if cLogSize > 0 {
		b := make([]byte, cLogEntrySize)

		_, err := cLog.ReadAt(b[:], cLogSize-int64(cLogEntrySize))
		if err != nil {
			return nil, fmt.Errorf("corrupted commit-log: could not read the last commit: %w", err)
		}

		committedTxOffset = int64(binary.BigEndian.Uint64(b[:]))
		committedTxSize = int(binary.BigEndian.Uint32(b[txIDSize:]))
		committedTxLogSize = committedTxOffset + int64(committedTxSize)
		committedTxID = uint64(cLogSize) / uint64(cLogEntrySize)

		if cLogEntrySize == cLogEntrySizeV2 {
			copy(committedAlh[:], b[txIDSize+lszSize:])
		}

		txLogFileSize, err := txLog.Size()
		if err != nil {
			return nil, fmt.Errorf("corrupted transaction log: could not get size: %w", err)
		}

		if txLogFileSize < committedTxLogSize {
			return nil, fmt.Errorf("corrupted transaction log: size is too small: %w", ErrCorruptedTxData)
		}
	}

	txPool, err := newTxPool(txPoolOptions{
		poolSize:     opts.MaxConcurrency,
		maxTxEntries: maxTxEntries,
		maxKeyLen:    maxKeyLen,
		preallocated: true,
	})
	if err != nil {
		return nil, fmt.Errorf("invalid configuration, couldn't initialize transaction holder pool")
	}

	maxTxSize := maxTxSize(maxTxEntries, maxKeyLen, maxTxMetadataLen, maxKVMetadataLen)
	txbs := make([]byte, maxTxSize)

	if cLogSize > 0 {
		txReader := appendable.NewReaderFrom(txLog, committedTxOffset, committedTxSize)

		tx, _ := txPool.Alloc()

		err = tx.readFrom(txReader, false)
		if err != nil {
			txPool.Release(tx)
			return nil, fmt.Errorf("corrupted transaction log: could not read the last transaction: %w", err)
		}

		txPool.Release(tx)

		if cLogEntrySize == cLogEntrySizeV1 {
			committedAlh = tx.header.Alh()
		}

		if cLogEntrySize == cLogEntrySizeV2 {
			if committedAlh != tx.header.Alh() {
				return nil, fmt.Errorf("corrupted transaction log: digest mismatch in the last transaction: %w", err)
			}
		}
	}

	cLogBuf := newPrecommitBuffer(opts.MaxActiveTransactions)

	precommittedTxID := committedTxID
	precommittedAlh := committedAlh
	precommittedTxLogSize := committedTxLogSize

	if !opts.DiscardPrecommittedTransactions {
		// read pre-committed txs from txLog and insert into cLogBuf to continue with the commit process
		// txLog may be partially written, precommitted transactions loading is terminated if an inconsistency is found
		txReader := appendable.NewReaderFrom(txLog, precommittedTxLogSize, multiapp.DefaultReadBufferSize)

		tx, _ := txPool.Alloc()

		for {
			err = tx.readFrom(txReader, false)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				opts.logger.Infof("%v: discarding pre-committed transaction: %d", err, precommittedTxID+1)
				break
			}

			if tx.header.ID != precommittedTxID+1 || tx.header.PrevAlh != precommittedAlh {
				opts.logger.Infof("%v: discarding pre-committed transaction: %d", ErrCorruptedData, precommittedTxID+1)
				break
			}

			precommittedTxID++
			precommittedAlh = tx.header.Alh()

			txSize := int(txReader.ReadCount() - (precommittedTxLogSize - committedTxLogSize))

			err = cLogBuf.put(precommittedTxID, precommittedAlh, precommittedTxLogSize, txSize)
			if err != nil {
				txPool.Release(tx)
				return nil, fmt.Errorf("%v: while loading pre-committed transaction: %v", err, precommittedTxID+1)
			}

			precommittedTxLogSize += int64(txSize)
		}

		txPool.Release(tx)
	}

	vLogsMap := make(map[byte]*refVLog, len(vLogs))
	vLogUnlockedList := list.New()

	for i, vLog := range vLogs {
		e := vLogUnlockedList.PushBack(byte(i))
		vLogsMap[byte(i)] = &refVLog{vLog: vLog, unlockedRef: e}
	}

	var vLogCache *cache.Cache

	if opts.VLogCacheSize > 0 {
		vLogCache, err = cache.NewCache(opts.VLogCacheSize)
		if err != nil {
			return nil, err
		}
	}

	ahtPath := filepath.Join(path, ahtDirname)

	ahtOpts := ahtree.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithFileSize(fileSize).
		WithRetryableSync(opts.Synced).
		WithAutoSync(true).
		WithWriteBufferSize(opts.AHTOpts.WriteBufferSize).
		WithSyncThld(opts.AHTOpts.SyncThld)

	if opts.appFactory != nil {
		ahtOpts.WithAppFactory(func(rootPath, subPath string, appOpts *multiapp.Options) (appendable.Appendable, error) {
			return opts.appFactory(path, filepath.Join(ahtDirname, subPath), appOpts)
		})
	}

	aht, err := ahtree.Open(ahtPath, ahtOpts)
	if err != nil {
		return nil, fmt.Errorf("could not open aht: %w", err)
	}

	txLogCache, err := cache.NewCache(opts.TxLogCacheSize) // TODO: optionally it could include up to opts.MaxActiveTransactions upon start
	if err != nil {
		return nil, err
	}

	store := &ImmuStore{
		path:             path,
		logger:           opts.logger,
		txLog:            txLog,
		txLogCache:       txLogCache,
		vLogs:            vLogsMap,
		vLogUnlockedList: vLogUnlockedList,
		vLogsCond:        sync.NewCond(&sync.Mutex{}),
		vLogCache:        vLogCache,

		cLog:          cLog,
		cLogEntrySize: cLogEntrySize,

		cLogBuf: cLogBuf,

		committedTxID: committedTxID,
		committedAlh:  committedAlh,

		inmemPrecommittedTxID: precommittedTxID,
		inmemPrecommittedAlh:  precommittedAlh,
		precommittedTxLogSize: precommittedTxLogSize,

		embeddedValues: embeddedValues,
		preallocFiles:  preallocFiles,

		readOnly:              opts.ReadOnly,
		synced:                opts.Synced,
		syncFrequency:         opts.SyncFrequency,
		maxActiveTransactions: opts.MaxActiveTransactions,
		mvccReadSetLimit:      opts.MVCCReadSetLimit,
		maxWaitees:            opts.MaxWaitees,
		maxConcurrency:        opts.MaxConcurrency,
		maxIOConcurrency:      opts.MaxIOConcurrency,

		maxTxEntries: maxTxEntries,
		maxKeyLen:    maxKeyLen,
		maxValueLen:  maxValueLen,

		writeTxHeaderVersion: opts.WriteTxHeaderVersion,

		timeFunc:                   opts.TimeFunc,
		multiIndexing:              opts.MultiIndexing,
		indexers:                   make(map[[sha256.Size]byte]*indexer),
		memSemaphore:               semaphore.New(uint64(opts.IndexOpts.MaxGlobalBufferedDataSize)),
		useExternalCommitAllowance: opts.UseExternalCommitAllowance,
		commitAllowedUpToTxID:      committedTxID,

		aht: aht,

		inmemPrecommitWHub:   watchers.New(0, opts.MaxActiveTransactions+1), // syncer (TODO: indexer may wait here instead)
		durablePrecommitWHub: watchers.New(0, opts.MaxActiveTransactions+opts.MaxWaitees),
		commitWHub:           watchers.New(0, 1+opts.MaxActiveTransactions+opts.MaxWaitees), // including indexer

		txPool: txPool,
		_txbs:  txbs,

		opts: opts,

		compactionDisabled: opts.CompactionDisabled,
	}

	if store.aht.Size() > precommittedTxID {
		err = store.aht.ResetSize(precommittedTxID)
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("corrupted commit-log: can not truncate aht tree: %w", err)
		}
	}

	if store.aht.Size() == precommittedTxID {
		store.logger.Infof("binary-linking up to date at '%s'", store.path)
	} else {
		err = store.syncBinaryLinking()
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("binary-linking syncing failed: %w", err)
		}
	}

	err = store.inmemPrecommitWHub.DoneUpto(precommittedTxID)
	if err != nil {
		store.Close()
		return nil, err
	}

	err = store.durablePrecommitWHub.DoneUpto(precommittedTxID)
	if err != nil {
		store.Close()
		return nil, err
	}

	err = store.commitWHub.DoneUpto(committedTxID)
	if err != nil {
		store.Close()
		return nil, err
	}

	if !store.multiIndexing {
		err := store.InitIndexing(&IndexSpec{})
		if err != nil {
			store.Close()
			return nil, err
		}
	}

	if store.synced {
		go store.syncer()
	}

	return store, nil
}

func (s *ImmuStore) syncer() {
	for {
		committedTxID := s.LastCommittedTxID()

		// passive wait for one new transaction at least
		err := s.inmemPrecommitWHub.WaitFor(context.Background(), committedTxID+1)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return
		}

		// TODO: waiting on earlier stages of transaction processing may also be possible
		prevLatestPrecommitedTx := committedTxID + 1

		// TODO: parametrize concurrency evaluation
		for i := 0; i < 4; i++ {
			// give some time for more transactions to be precommitted
			time.Sleep(s.syncFrequency / 4)

			latestPrecommitedTx := s.LastPrecommittedTxID()

			if prevLatestPrecommitedTx == latestPrecommitedTx {
				// avoid waiting if there are no new transactions
				break
			}

			prevLatestPrecommitedTx = latestPrecommitedTx
		}

		// ensure durability
		err = s.sync()
		if errors.Is(err, ErrAlreadyClosed) ||
			errors.Is(err, multiapp.ErrAlreadyClosed) ||
			errors.Is(err, singleapp.ErrAlreadyClosed) ||
			errors.Is(err, watchers.ErrAlreadyClosed) {
			return
		}
		if err != nil {
			s.notify(Error, true, "%s: while syncing transactions", err)
		}
	}
}

type NotificationType = int

const NotificationWindow = 60 * time.Second
const (
	Info NotificationType = iota
	Warn
	Error
)

func (s *ImmuStore) notify(nType NotificationType, mandatory bool, formattedMessage string, args ...interface{}) {
	s.notifyMutex.Lock()
	defer s.notifyMutex.Unlock()

	if mandatory || time.Since(s.lastNotification) > NotificationWindow {
		switch nType {
		case Info:
			{
				s.logger.Infof(formattedMessage, args...)
			}
		case Warn:
			{
				s.logger.Warningf(formattedMessage, args...)
			}
		case Error:
			{
				s.logger.Errorf(formattedMessage, args...)
			}
		}
		s.lastNotification = time.Now()
	}
}

func hasPrefix(key, prefix []byte) bool {
	return len(key) >= len(prefix) && bytes.Equal(prefix, key[:len(prefix)])
}

func (s *ImmuStore) getIndexerFor(keyPrefix []byte) (*indexer, error) {
	s.indexersMux.RLock()
	defer s.indexersMux.RUnlock()

	for _, indexer := range s.indexers {
		if hasPrefix(keyPrefix, indexer.TargetPrefix()) {
			return indexer, nil
		}
	}
	return nil, ErrIndexNotFound
}

type IndexSpec struct {
	SourcePrefix      []byte
	SourceEntryMapper EntryMapper

	TargetEntryMapper EntryMapper
	TargetPrefix      []byte

	InjectiveMapping bool

	InitialTxID uint64
	FinalTxID   uint64
	InitialTs   int64
	FinalTs     int64
}

func (s *ImmuStore) InitIndexing(spec *IndexSpec) error {
	if spec == nil {
		return ErrIllegalArguments
	}

	if len(spec.TargetPrefix) == 0 && len(spec.SourcePrefix) > 0 {
		return fmt.Errorf("%w: empty prefix can not have a source prefix", ErrIllegalArguments)
	}

	s.indexersMux.Lock()
	defer s.indexersMux.Unlock()

	indexPrefix := sha256.Sum256(spec.TargetPrefix)

	_, ok := s.indexers[indexPrefix]
	if ok {
		return ErrIndexAlreadyInitialized
	}

	var indexPath string

	if len(spec.TargetPrefix) == 0 {
		indexPath = filepath.Join(s.path, indexDirname)
	} else {
		encPrefix := hex.EncodeToString(spec.TargetPrefix)
		indexPath = filepath.Join(s.path, fmt.Sprintf("%s_%s", indexDirname, encPrefix))
	}

	if s.indexCache == nil {
		c, err := cache.NewCache(s.opts.IndexOpts.CacheSize)
		if err != nil {
			return err
		}
		s.indexCache = c
	}

	indexer, err := newIndexer(indexPath, s, s.opts)
	if err != nil {
		return fmt.Errorf("%w: could not open indexer", err)
	}

	if indexer.Ts() > s.LastCommittedTxID() {
		return fmt.Errorf("%w: index size is too large", ErrCorruptedIndex)

		// TODO: if indexing is done on pre-committed txs, the index may be rollback to a previous snapshot where it was already synced
		// NOTE: compaction should preserve snapshot which are not synced... so to ensure rollback can be achieved
	}

	s.indexers[indexPrefix] = indexer
	indexer.init(spec)

	return nil
}

func (s *ImmuStore) CloseIndexing(prefix []byte) error {
	s.indexersMux.Lock()
	defer s.indexersMux.Unlock()

	indexPrefix := sha256.Sum256(prefix)

	indexer, ok := s.indexers[indexPrefix]
	if !ok {
		return fmt.Errorf("%w: index not found", ErrIndexNotFound)
	}

	err := indexer.Close()
	if err != nil {
		return err
	}

	delete(s.indexers, indexPrefix)

	return nil
}

func (s *ImmuStore) DeleteIndex(prefix []byte) error {
	s.indexersMux.Lock()
	defer s.indexersMux.Unlock()

	indexPrefix := sha256.Sum256(prefix)

	indexer, ok := s.indexers[indexPrefix]
	if !ok {
		return fmt.Errorf("%w: index not found", ErrIndexNotFound)
	}

	indexer.Close()

	delete(s.indexers, indexPrefix)

	s.logger.Infof("deleting index path: '%s' ...", indexer.path)

	return os.RemoveAll(indexer.path)
}

func (s *ImmuStore) GetBetween(ctx context.Context, key []byte, initialTxID uint64, finalTxID uint64) (valRef ValueRef, err error) {
	indexer, err := s.getIndexerFor(key)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	indexedVal, tx, hc, err := indexer.GetBetween(key, initialTxID, finalTxID)
	if err != nil {
		return nil, err
	}

	return s.valueRefFrom(tx, hc, indexedVal)
}

func (s *ImmuStore) Get(ctx context.Context, key []byte) (valRef ValueRef, err error) {
	return s.GetWithFilters(ctx, key, IgnoreExpired, IgnoreDeleted)
}

func (s *ImmuStore) GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (valRef ValueRef, err error) {
	indexer, err := s.getIndexerFor(key)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	indexedVal, tx, hc, err := indexer.Get(key)
	if err != nil {
		return nil, err
	}

	valRef, err = s.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, err
	}

	now := time.Now()

	for _, filter := range filters {
		if filter == nil {
			return nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}

		err = filter(valRef, now)
		if err != nil {
			return nil, err
		}
	}

	return valRef, nil
}

func (s *ImmuStore) GetWithPrefix(ctx context.Context, prefix []byte, neq []byte) (key []byte, valRef ValueRef, err error) {
	return s.GetWithPrefixAndFilters(ctx, prefix, neq, IgnoreExpired, IgnoreDeleted)
}

func (s *ImmuStore) GetWithPrefixAndFilters(ctx context.Context, prefix []byte, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error) {
	indexer, err := s.getIndexerFor(prefix)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, nil, ErrKeyNotFound
		}

		return nil, nil, err
	}

	key, indexedVal, tx, hc, err := indexer.GetWithPrefix(prefix, neq)
	if err != nil {
		return nil, nil, err
	}

	valRef, err = s.valueRefFrom(tx, hc, indexedVal)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()

	for _, filter := range filters {
		if filter == nil {
			return nil, nil, fmt.Errorf("%w: invalid filter function", ErrIllegalArguments)
		}

		err = filter(valRef, now)
		if err != nil {
			return nil, nil, err
		}
	}

	return key, valRef, nil
}

func (s *ImmuStore) History(key []byte, offset uint64, descOrder bool, limit int) (valRefs []ValueRef, hCount uint64, err error) {
	indexer, err := s.getIndexerFor(key)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, 0, ErrKeyNotFound
		}

		return nil, 0, err
	}

	timedValues, hCount, err := indexer.History(key, offset, descOrder, limit)
	if err != nil {
		return nil, 0, err
	}

	valRefs = make([]ValueRef, len(timedValues))

	rev := offset + 1
	if descOrder {
		rev = hCount - offset
	}

	for i, timedValue := range timedValues {
		val, err := s.valueRefFrom(timedValue.Ts, rev, timedValue.Value)
		if err != nil {
			return nil, 0, err
		}

		valRefs[i] = val

		if descOrder {
			rev--
		} else {
			rev++
		}
	}

	return valRefs, hCount, nil
}

func (s *ImmuStore) MultiIndexingEnabled() bool {
	return s.multiIndexing
}

func (s *ImmuStore) UseTimeFunc(timeFunc TimeFunc) error {
	if timeFunc == nil {
		return ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.timeFunc = timeFunc

	return nil
}

func (s *ImmuStore) NewTxHolderPool(poolSize int, preallocated bool) (TxPool, error) {
	return newTxPool(txPoolOptions{
		poolSize:     poolSize,
		maxTxEntries: s.maxTxEntries,
		maxKeyLen:    s.maxKeyLen,
		preallocated: preallocated,
	})
}

func (s *ImmuStore) syncSnapshot(prefix []byte) (*Snapshot, error) {
	indexer, err := s.getIndexerFor(prefix)
	if err != nil {
		return nil, err
	}

	snap, err := indexer.SyncSnapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:     s,
		prefix: prefix,
		snap:   snap,
		ts:     time.Now(),
	}, nil
}

func (s *ImmuStore) Snapshot(prefix []byte) (*Snapshot, error) {
	indexer, err := s.getIndexerFor(prefix)
	if err != nil {
		return nil, err
	}

	snap, err := indexer.Snapshot()
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:     s,
		prefix: prefix,
		snap:   snap,
		ts:     time.Now(),
	}, nil
}

// SnapshotMustIncludeTxID returns a new snapshot based on an existent dumped root (snapshot reuse).
// Current root may be dumped if there are no previous root already stored on disk or if the dumped one was old enough.
// If txID is 0, any snapshot may be used.
func (s *ImmuStore) SnapshotMustIncludeTxID(ctx context.Context, prefix []byte, txID uint64) (*Snapshot, error) {
	return s.SnapshotMustIncludeTxIDWithRenewalPeriod(ctx, prefix, txID, 0)
}

// SnapshotMustIncludeTxIDWithRenewalPeriod returns a new snapshot based on an existent dumped root (snapshot reuse).
// Current root may be dumped if there are no previous root already stored on disk or if the dumped one was old enough.
// If txID is 0, any snapshot not older than renewalPeriod may be used.
// If renewalPeriod is 0, renewal period is not taken into consideration
func (s *ImmuStore) SnapshotMustIncludeTxIDWithRenewalPeriod(ctx context.Context, prefix []byte, txID uint64, renewalPeriod time.Duration) (*Snapshot, error) {
	indexer, err := s.getIndexerFor(prefix)
	if err != nil {
		return nil, err
	}

	err = indexer.WaitForIndexingUpto(ctx, txID)
	if err != nil {
		return nil, err
	}

	snap, err := indexer.SnapshotMustIncludeTxIDWithRenewalPeriod(ctx, txID, renewalPeriod)
	if err != nil {
		return nil, err
	}

	return &Snapshot{
		st:     s,
		prefix: indexer.TargetPrefix(),
		snap:   snap,
		ts:     time.Now(),
	}, nil
}

func (s *ImmuStore) CommittedAlh() (uint64, [sha256.Size]byte) {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.committedTxID, s.committedAlh
}

func (s *ImmuStore) PrecommittedAlh() (uint64, [sha256.Size]byte) {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	durablePrecommittedTxID, _, _ := s.durablePrecommitWHub.Status()

	if durablePrecommittedTxID == s.committedTxID {
		return s.committedTxID, s.committedAlh
	}

	if durablePrecommittedTxID == s.inmemPrecommittedTxID {
		return s.inmemPrecommittedTxID, s.inmemPrecommittedAlh
	}

	// fetch latest precommitted (durable) transaction from s.cLogBuf
	txID, alh, _, _, _ := s.cLogBuf.readAhead(int(durablePrecommittedTxID - s.committedTxID - 1))

	return txID, alh
}

func (s *ImmuStore) precommittedAlh() (uint64, [sha256.Size]byte) {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.inmemPrecommittedTxID, s.inmemPrecommittedAlh
}

func (s *ImmuStore) syncBinaryLinking() error {
	s.logger.Infof("syncing binary-linking at '%s'...", s.path)

	tx, err := s.fetchAllocTx()
	if err != nil {
		return err
	}
	defer s.releaseAllocTx(tx)

	txReader, err := s.newTxReader(s.aht.Size()+1, false, true, false, tx)
	if err != nil {
		return err
	}

	for {
		tx, err := txReader.Read()
		if errors.Is(err, ErrNoMoreEntries) {
			break
		}
		if err != nil {
			return err
		}

		alh := tx.header.Alh()
		s.aht.Append(alh[:])

		if tx.header.ID%1000 == 0 {
			s.logger.Infof("binary-linking at '%s' in progress: processing tx: %d", s.path, tx.header.ID)
		}
	}

	s.logger.Infof("binary-linking up to date at '%s'", s.path)

	return nil
}

func (s *ImmuStore) WaitForTx(ctx context.Context, txID uint64, allowPrecommitted bool) error {
	s.waiteesMutex.Lock()

	if s.waiteesCount == s.maxWaitees {
		s.waiteesMutex.Unlock()
		return watchers.ErrMaxWaitessLimitExceeded
	}

	s.waiteesCount++

	s.waiteesMutex.Unlock()

	defer func() {
		s.waiteesMutex.Lock()
		s.waiteesCount--
		s.waiteesMutex.Unlock()
	}()

	var err error

	if allowPrecommitted {
		err = s.durablePrecommitWHub.WaitFor(ctx, txID)
	} else {
		err = s.commitWHub.WaitFor(ctx, txID)
	}
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return ErrAlreadyClosed
	}
	return err
}

func (s *ImmuStore) WaitForIndexingUpto(ctx context.Context, txID uint64) error {
	s.waiteesMutex.Lock()

	if s.waiteesCount == s.maxWaitees {
		s.waiteesMutex.Unlock()
		return watchers.ErrMaxWaitessLimitExceeded
	}

	s.waiteesCount++

	s.waiteesMutex.Unlock()

	defer func() {
		s.waiteesMutex.Lock()
		s.waiteesCount--
		s.waiteesMutex.Unlock()
	}()

	for _, indexer := range s.indexers {
		err := indexer.WaitForIndexingUpto(ctx, txID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *ImmuStore) CompactIndexes() error {
	if s.compactionDisabled {
		return ErrCompactionDisabled
	}

	s.indexersMux.RLock()
	defer s.indexersMux.RUnlock()

	// TODO: indexes may be concurrently compacted

	for _, indexer := range s.indexers {
		err := indexer.CompactIndex()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *ImmuStore) FlushIndexes(cleanupPercentage float32, synced bool) error {
	s.indexersMux.RLock()
	defer s.indexersMux.RUnlock()

	// TODO: indexes may be concurrently flushed

	for _, indexer := range s.indexers {
		err := indexer.FlushIndex(cleanupPercentage, synced)
		if err != nil {
			return err
		}
	}

	return nil
}

func maxTxSize(maxTxEntries, maxKeyLen, maxTxMetadataLen, maxKVMetadataLen int) int {
	return txIDSize /*txID*/ +
		tsSize /*ts*/ +
		txIDSize /*blTxID*/ +
		sha256.Size /*blRoot*/ +
		sha256.Size /*prevAlh*/ +
		sszSize /*versioin*/ +
		sszSize /*txMetadataLen*/ +
		maxTxMetadataLen +
		lszSize /*|entries|*/ +
		maxTxEntries*(sszSize /*kvMetadataLen*/ +
			maxKVMetadataLen+
			sszSize /*kLen*/ +
			maxKeyLen /*key*/ +
			lszSize /*vLen*/ +
			offsetSize /*vOff*/ +
			sha256.Size /*hValue*/) +
		sha256.Size /*eH*/ +
		sha256.Size /*txH*/
}

func (s *ImmuStore) ReadOnly() bool {
	return s.readOnly
}

func (s *ImmuStore) Synced() bool {
	return s.synced
}

func (s *ImmuStore) MaxActiveTransactions() int {
	return s.maxActiveTransactions
}

func (s *ImmuStore) MVCCReadSetLimit() int {
	return s.mvccReadSetLimit
}

func (s *ImmuStore) MaxConcurrency() int {
	return s.maxConcurrency
}

func (s *ImmuStore) MaxIOConcurrency() int {
	return s.maxIOConcurrency
}

func (s *ImmuStore) MaxTxEntries() int {
	return s.maxTxEntries
}

func (s *ImmuStore) MaxKeyLen() int {
	return s.maxKeyLen
}

func (s *ImmuStore) MaxValueLen() int {
	return s.maxValueLen
}

func (s *ImmuStore) Size() (uint64, error) {
	var size uint64

	err := filepath.WalkDir(s.path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			size += uint64(info.Size())
		}
		return nil
	})
	return size, err
}

func (s *ImmuStore) TxCount() uint64 {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.committedTxID
}

func (s *ImmuStore) fetchAllocTx() (*Tx, error) {
	tx, err := s.txPool.Alloc()
	if errors.Is(err, ErrTxPoolExhausted) {
		return nil, ErrMaxConcurrencyLimitExceeded
	}
	return tx, nil
}

func (s *ImmuStore) releaseAllocTx(tx *Tx) {
	s.txPool.Release(tx)
}

func encodeOffset(offset int64, vLogID byte) int64 {
	return int64(vLogID)<<56 | offset
}

func decodeOffset(offset int64) (byte, int64) {
	return byte(offset >> 56), offset & ^(0xff << 55)
}

func (s *ImmuStore) fetchAnyVLog() (vLodID byte, vLog appendable.Appendable) {
	s.vLogsCond.L.Lock()
	defer s.vLogsCond.L.Unlock()

	for s.vLogUnlockedList.Len() == 0 {
		s.vLogsCond.Wait()
	}

	vLogID := s.vLogUnlockedList.Remove(s.vLogUnlockedList.Front()).(byte) + 1
	s.vLogs[vLogID-1].unlockedRef = nil // locked

	return vLogID, s.vLogs[vLogID-1].vLog
}

func (s *ImmuStore) fetchVLog(vLogID byte) (appendable.Appendable, error) {
	if s.embeddedValues {
		if vLogID > 0 {
			return nil, fmt.Errorf("%w: attempt to read from a non-embedded vlog while using embedded values", ErrUnexpectedError)
		}

		s.commitStateRWMutex.Lock()
		return s.txLog, nil
	}

	s.vLogsCond.L.Lock()
	defer s.vLogsCond.L.Unlock()

	for s.vLogs[vLogID-1].unlockedRef == nil {
		s.vLogsCond.Wait()
	}

	s.vLogUnlockedList.Remove(s.vLogs[vLogID-1].unlockedRef)
	s.vLogs[vLogID-1].unlockedRef = nil // locked

	return s.vLogs[vLogID-1].vLog, nil
}

func (s *ImmuStore) releaseVLog(vLogID byte) error {
	if s.embeddedValues {
		if vLogID > 0 {
			return fmt.Errorf("%w: attempt to release a non-embedded vlog while using embedded values", ErrUnexpectedError)
		}

		s.commitStateRWMutex.Unlock()
		return nil
	}

	s.vLogsCond.L.Lock()
	s.vLogs[vLogID-1].unlockedRef = s.vLogUnlockedList.PushBack(vLogID - 1) // unlocked
	s.vLogsCond.L.Unlock()
	s.vLogsCond.Signal()

	return nil
}

type appendableResult struct {
	offsets []int64
	err     error
}

func (s *ImmuStore) appendValuesIntoAnyVLog(entries []*EntrySpec) (offsets []int64, err error) {
	vLogID, vLog := s.fetchAnyVLog()
	defer s.releaseVLog(vLogID)

	offsets, err = s.appendValuesInto(entries, vLog)
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(offsets); i++ {
		offsets[i] = encodeOffset(offsets[i], vLogID)
	}

	return offsets, nil
}

func (s *ImmuStore) appendValuesInto(entries []*EntrySpec, app appendable.Appendable) (offsets []int64, err error) {
	offsets = make([]int64, len(entries))

	for i := 0; i < len(offsets); i++ {
		if len(entries[i].Value) == 0 {
			continue
		}

		offsets[i], _, err = app.Append(entries[i].Value)
		if err != nil {
			return nil, err
		}
	}

	return offsets, nil
}

func (s *ImmuStore) NewWriteOnlyTx(ctx context.Context) (*OngoingTx, error) {
	return newOngoingTx(ctx, s, &TxOptions{Mode: WriteOnlyTx})
}

func (s *ImmuStore) NewTx(ctx context.Context, opts *TxOptions) (*OngoingTx, error) {
	return newOngoingTx(ctx, s, opts)
}

func (s *ImmuStore) commit(ctx context.Context, otx *OngoingTx, expectedHeader *TxHeader, skipIntegrityCheck bool, waitForIndexing bool) (*TxHeader, error) {
	hdr, err := s.precommit(ctx, otx, expectedHeader, skipIntegrityCheck)
	if err != nil {
		return nil, err
	}

	// note: durability is ensured only if the store is in sync mode
	err = s.commitWHub.WaitFor(ctx, hdr.ID)
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return nil, ErrAlreadyClosed
	}
	if err != nil {
		return nil, err
	}

	if waitForIndexing {
		err = s.WaitForIndexingUpto(ctx, hdr.ID)
		// header is returned because transaction is already committed
		if err != nil {
			return hdr, err
		}
	}

	return hdr, nil
}

func (s *ImmuStore) precommit(ctx context.Context, otx *OngoingTx, hdr *TxHeader, skipIntegrityCheck bool) (*TxHeader, error) {
	if otx == nil {
		return nil, fmt.Errorf("%w: no transaction", ErrIllegalArguments)
	}

	err := otx.validateAgainst(hdr)
	if err != nil {
		return nil, fmt.Errorf("%w: transaction does not validate against header", err)
	}

	// extra metadata are specified by the client and thus they are only allowed when entries is non empty
	if len(otx.entries) == 0 && (otx.metadata.IsEmpty() || otx.metadata.HasExtraOnly()) {
		return nil, ErrNoEntriesProvided
	}

	err = s.validateEntries(otx.entries)
	if err != nil {
		return nil, err
	}

	err = s.validatePreconditions(otx.preconditions)
	if err != nil {
		return nil, err
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	if hdr == nil {
		tx.header.Version = s.writeTxHeaderVersion
	} else {
		tx.header.Version = hdr.Version
	}

	tx.header.Metadata = otx.metadata

	tx.header.NEntries = len(otx.entries)

	doneWithValuesCh := make(chan appendableResult)
	defer close(doneWithValuesCh)

	go func() {
		// value write is delayed to ensure values are inmediatelly followed by the associated tx header
		if s.embeddedValues {
			doneWithValuesCh <- appendableResult{nil, nil}
			return
		}

		offsets, err := s.appendValuesIntoAnyVLog(otx.entries)
		if err != nil {
			doneWithValuesCh <- appendableResult{nil, err}
			return
		}

		doneWithValuesCh <- appendableResult{offsets, nil}
	}()

	for i, e := range otx.entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.md = e.Metadata
		txe.vLen = len(e.Value)
		if e.IsValueTruncated {
			txe.hVal = e.HashValue
		} else {
			txe.hVal = sha256.Sum256(e.Value)
		}
	}

	err = tx.BuildHashTree()
	if err != nil {
		<-doneWithValuesCh // wait for data to be written
		return nil, err
	}

	valueWritingResult := <-doneWithValuesCh // wait for data to be written
	if valueWritingResult.err != nil {
		return nil, valueWritingResult.err
	}

	if !s.embeddedValues {
		for i := 0; i < tx.header.NEntries; i++ {
			tx.entries[i].vOff = valueWritingResult.offsets[i]
		}
	}

	if hdr != nil {
		// TODO: Eh validation is disabled as it's not provided
		// when the tx was exported without integrity checks
		if !skipIntegrityCheck && tx.header.Eh != hdr.Eh {
			return nil, fmt.Errorf("%w: entries hash (Eh) differs", ErrIllegalArguments)
		}

		lastPreCommittedTxID := s.LastPrecommittedTxID()

		if lastPreCommittedTxID >= hdr.ID {
			return nil, ErrTxAlreadyCommitted
		}

		if hdr.ID > lastPreCommittedTxID+uint64(s.maxActiveTransactions) {
			return nil, ErrMaxActiveTransactionsLimitExceeded
		}

		// ensure tx is committed in the expected order
		err = s.inmemPrecommitWHub.WaitFor(ctx, hdr.ID-1)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return nil, ErrAlreadyClosed
		}
		if err != nil {
			return nil, err
		}

		var blRoot [sha256.Size]byte

		if hdr.BlTxID > 0 {
			blRoot, err = s.aht.RootAt(hdr.BlTxID)
			if err != nil && !errors.Is(err, ahtree.ErrEmptyTree) {
				return nil, err
			}
		}

		if blRoot != hdr.BlRoot {
			return nil, fmt.Errorf("%w: attempt to commit a tx with invalid blRoot", ErrIllegalArguments)
		}
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	currPrecomittedTxID, currPrecommittedAlh := s.precommittedAlh()

	var ts int64
	var blTxID uint64
	if hdr == nil {
		ts = s.timeFunc().Unix()
		blTxID = s.aht.Size()
	} else {
		ts = hdr.Ts
		blTxID = hdr.BlTxID

		// currTxID and currAlh were already checked before,
		// but we have to add an additional check once the commit mutex
		// is locked to ensure that those constraints are still valid
		// in case of simultaneous writers
		if currPrecomittedTxID > hdr.ID-1 {
			return nil, ErrTxAlreadyCommitted
		}

		if currPrecomittedTxID < hdr.ID-1 {
			return nil, fmt.Errorf("%w: attempt to commit a tx in wrong order", ErrUnexpectedError)
		}

		if currPrecommittedAlh != hdr.PrevAlh {
			return nil, fmt.Errorf("%w: attempt to commit a tx with invalid prevAlh", ErrIllegalArguments)
		}
	}

	if otx.hasPreconditions() {
		var waitForIndexingUpto uint64

		if otx.unsafeMVCC {
			waitForIndexingUpto = s.mandatoryMVCCUpToTxID
		} else {
			// Preconditions must be executed with up-to-date tree
			waitForIndexingUpto = currPrecomittedTxID
		}

		err = s.WaitForIndexingUpto(ctx, waitForIndexingUpto)
		if err != nil {
			return nil, err
		}

		err = otx.checkPreconditions(ctx, s)
		if err != nil {
			return nil, err
		}
	}

	err = s.performPrecommit(tx, otx.entries, ts, blTxID)
	if err != nil {
		return nil, err
	}

	if otx.requireMVCCOnFollowingTxs {
		s.mandatoryMVCCUpToTxID = tx.header.ID
	}

	return tx.Header(), err
}

func (s *ImmuStore) LastCommittedTxID() uint64 {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.committedTxID
}

func (s *ImmuStore) LastPrecommittedTxID() uint64 {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.inmemPrecommittedTxID
}

func (s *ImmuStore) MandatoryMVCCUpToTxID() uint64 {
	s.commitStateRWMutex.RLock()
	defer s.commitStateRWMutex.RUnlock()

	return s.mandatoryMVCCUpToTxID
}

func (s *ImmuStore) performPrecommit(tx *Tx, entries []*EntrySpec, ts int64, blTxID uint64) error {
	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	// limit the maximum number of pre-committed transactions
	if s.synced && s.committedTxID+uint64(s.maxActiveTransactions) <= s.inmemPrecommittedTxID {
		return ErrMaxActiveTransactionsLimitExceeded
	}

	// will overwrite partially written and uncommitted data
	err := s.txLog.SetOffset(s.precommittedTxLogSize)
	if err != nil {
		return fmt.Errorf("commit-log: could not set offset: %w", err)
	}

	tx.header.ID = s.inmemPrecommittedTxID + 1
	tx.header.Ts = ts

	tx.header.BlTxID = blTxID

	if blTxID > 0 {
		blRoot, err := s.aht.RootAt(blTxID)
		if err != nil && !errors.Is(err, ahtree.ErrEmptyTree) {
			return err
		}
		tx.header.BlRoot = blRoot
	}

	if tx.header.ID <= tx.header.BlTxID {
		return ErrUnexpectedLinkingError
	}

	tx.header.PrevAlh = s.inmemPrecommittedAlh

	txSize := 0

	// tx serialization into pre-allocated buffer
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.header.ID))
	txSize += txIDSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.header.Ts))
	txSize += tsSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.header.BlTxID))
	txSize += txIDSize
	copy(s._txbs[txSize:], tx.header.BlRoot[:])
	txSize += sha256.Size
	copy(s._txbs[txSize:], tx.header.PrevAlh[:])
	txSize += sha256.Size

	binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(tx.header.Version))
	txSize += sszSize

	switch tx.header.Version {
	case 0:
		{
			binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(tx.header.NEntries))
			txSize += sszSize
		}
	case 1:
		{
			var txmdbs []byte

			if tx.header.Metadata != nil {
				txmdbs = tx.header.Metadata.Bytes()
			}

			binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(len(txmdbs)))
			txSize += sszSize

			copy(s._txbs[txSize:], txmdbs)
			txSize += len(txmdbs)

			binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(tx.header.NEntries))
			txSize += lszSize
		}
	default:
		{
			panic(fmt.Errorf("missing tx serialization method for version %d", tx.header.Version))
		}
	}

	// will overwrite partially written and uncommitted data
	err = s.txLog.SetOffset(s.precommittedTxLogSize)
	if err != nil {
		return fmt.Errorf("%w: could not set offset in txLog", err)
	}

	// total values length will be prefixed when values are embedded into txLog
	txPrefixLen := 0

	if s.embeddedValues {
		embeddedValuesLen := 0
		for _, e := range entries {
			embeddedValuesLen += len(e.Value)
		}

		var embeddedValuesLenBs [sszSize]byte
		binary.BigEndian.PutUint16(embeddedValuesLenBs[:], uint16(embeddedValuesLen))

		_, _, err := s.txLog.Append(embeddedValuesLenBs[:])
		if err != nil {
			return fmt.Errorf("%w: writing transaction values", err)
		}

		offsets, err := s.appendValuesInto(entries, s.txLog)
		if err != nil {
			return fmt.Errorf("%w: writing transaction values", err)
		}

		for i := 0; i < tx.header.NEntries; i++ {
			tx.entries[i].vOff = offsets[i]
		}

		txPrefixLen = sszSize + embeddedValuesLen
	}

	for i := 0; i < tx.header.NEntries; i++ {
		txe := tx.entries[i]

		// tx serialization using pre-allocated buffer
		// md is stored before key to ensure backward compatibility
		var kvmdbs []byte

		if txe.md != nil {
			kvmdbs = txe.md.Bytes()
		}

		binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(len(kvmdbs)))
		txSize += sszSize
		copy(s._txbs[txSize:], kvmdbs)
		txSize += len(kvmdbs)
		binary.BigEndian.PutUint16(s._txbs[txSize:], uint16(txe.kLen))
		txSize += sszSize
		copy(s._txbs[txSize:], txe.k[:txe.kLen])
		txSize += txe.kLen
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(txe.vLen))
		txSize += lszSize
		binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(txe.vOff))
		txSize += offsetSize
		copy(s._txbs[txSize:], txe.hVal[:])
		txSize += sha256.Size
	}

	// tx serialization using pre-allocated buffer
	alh := tx.header.Alh()

	copy(s._txbs[txSize:], alh[:])
	txSize += sha256.Size

	txbs := make([]byte, txSize)
	copy(txbs, s._txbs[:txSize])

	txOff, _, err := s.txLog.Append(txbs)
	if err != nil {
		return err
	}

	_, _, err = s.txLogCache.Put(tx.header.ID, txbs)
	if err != nil {
		return err
	}

	err = s.aht.ResetSize(s.inmemPrecommittedTxID)
	if err != nil {
		return err
	}
	_, _, err = s.aht.Append(alh[:])
	if err != nil {
		return err
	}

	err = s.cLogBuf.put(s.inmemPrecommittedTxID+1, alh, txOff, txSize)
	if err != nil {
		return err
	}

	s.inmemPrecommittedTxID++
	s.inmemPrecommittedAlh = alh
	s.precommittedTxLogSize += int64(txPrefixLen + txSize)

	s.inmemPrecommitWHub.DoneUpto(s.inmemPrecommittedTxID)

	if !s.synced {
		s.durablePrecommitWHub.DoneUpto(s.inmemPrecommittedTxID)

		return s.mayCommit()
	}

	return nil
}

func (s *ImmuStore) SetExternalCommitAllowance(enabled bool) {
	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	s.useExternalCommitAllowance = enabled

	if enabled {
		s.commitAllowedUpToTxID = s.committedTxID
	}
}

// DiscardPrecommittedTxsSince discard precommitted txs
// No truncation is made into txLog which means, if the store is reopened
// some precommitted transactions may be reloaded.
// Discarding may need to be redone after re-opening the store.
func (s *ImmuStore) DiscardPrecommittedTxsSince(txID uint64) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return 0, ErrAlreadyClosed
	}

	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	if txID == 0 {
		return 0, fmt.Errorf("%w: invalid transaction ID", ErrIllegalArguments)
	}

	if txID <= s.committedTxID {
		return 0, fmt.Errorf("%w: only precommitted transactions can be discarded", ErrIllegalArguments)
	}

	if txID > s.inmemPrecommittedTxID {
		return 0, nil
	}

	txsToDiscard := int(s.inmemPrecommittedTxID + 1 - txID)

	err := s.aht.ResetSize(s.aht.Size() - uint64(txsToDiscard))
	if err != nil {
		return 0, err
	}

	// s.cLogBuf inludes all precommitted transactions (even durable ones)
	err = s.cLogBuf.recedeWriter(txsToDiscard)
	if err != nil {
		return 0, err
	}

	defer func() {
		durablePrecommittedTxID, _, _ := s.durablePrecommitWHub.Status()
		if durablePrecommittedTxID > s.inmemPrecommittedTxID {
			s.durablePrecommitWHub.RecedeTo(s.inmemPrecommittedTxID)
		}
	}()

	if txID-1 == s.committedTxID {
		s.inmemPrecommittedTxID = s.committedTxID
		s.inmemPrecommittedAlh = s.committedAlh
		return txsToDiscard, nil
	}

	tx, alh, _, _, err := s.cLogBuf.readAhead(int(s.inmemPrecommittedTxID-s.committedTxID-1) - txsToDiscard)
	if err != nil || tx != txID-1 {
		s.inmemPrecommittedTxID = s.committedTxID
		s.inmemPrecommittedAlh = s.committedAlh
		s.logger.Warningf("precommitted transactions has been discarded due to unexpected error in cLogBuf")
		return 0, err
	}

	s.inmemPrecommittedTxID = txID - 1
	s.inmemPrecommittedAlh = alh

	return txsToDiscard, nil
}

func (s *ImmuStore) AllowCommitUpto(txID uint64) error {
	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	if !s.useExternalCommitAllowance {
		return fmt.Errorf("%w: the external commit allowance mode is not enabled", ErrIllegalState)
	}

	if txID <= s.commitAllowedUpToTxID {
		// once a commit is allowed, it cannot be revoked
		return nil
	}

	if s.inmemPrecommittedTxID < txID {
		// commit allowances apply only to pre-committed transactions
		s.commitAllowedUpToTxID = s.inmemPrecommittedTxID
	} else {
		s.commitAllowedUpToTxID = txID
	}

	if !s.synced {
		return s.mayCommit()
	}

	return nil
}

// commitAllowedUpTo requires the caller to have already acquired the commitStateRWMutex lock
func (s *ImmuStore) commitAllowedUpTo() uint64 {
	if !s.useExternalCommitAllowance {
		return s.inmemPrecommittedTxID
	}

	return s.commitAllowedUpToTxID
}

// commitAllowedUpTo requires the caller to have already acquired the commitStateRWMutex lock
func (s *ImmuStore) mayCommit() error {
	commitAllowedUpToTxID := s.commitAllowedUpTo()
	txsCountToBeCommitted := int(commitAllowedUpToTxID - s.committedTxID)

	if txsCountToBeCommitted == 0 {
		return nil
	}

	// will overwrite partially written and uncommitted data
	err := s.cLog.SetOffset(int64(s.committedTxID) * int64(s.cLogEntrySize))
	if err != nil {
		return err
	}

	var commitUpToTxID uint64
	var commitUpToTxAlh [sha256.Size]byte

	for i := 0; i < txsCountToBeCommitted; i++ {
		txID, alh, txOff, txSize, err := s.cLogBuf.readAhead(i)
		if err != nil {
			return err
		}

		cb := make([]byte, s.cLogEntrySize)
		binary.BigEndian.PutUint64(cb, uint64(txOff))
		binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))

		if s.cLogEntrySize == cLogEntrySizeV2 {
			copy(cb[offsetSize+lszSize:], alh[:])
		}

		_, _, err = s.cLog.Append(cb)
		if err != nil {
			return err
		}

		commitUpToTxID = txID
		commitUpToTxAlh = alh
	}

	// added as a safety fuse but this situation should NOT happen
	if commitUpToTxID != commitAllowedUpToTxID {
		return fmt.Errorf("%w: may commit up to %d but actual transaction to be committed is %d", ErrUnexpectedError, commitAllowedUpToTxID, commitUpToTxID)
	}

	err = s.cLog.Flush()
	if err != nil {
		return err
	}

	err = s.cLogBuf.advanceReader(txsCountToBeCommitted)
	if err != nil {
		return err
	}

	s.committedTxID = commitUpToTxID
	s.committedAlh = commitUpToTxAlh

	s.commitWHub.DoneUpto(commitUpToTxID)

	return nil
}

func (s *ImmuStore) CommitWith(ctx context.Context, callback func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error), waitForIndexing bool) (*TxHeader, error) {
	hdr, err := s.preCommitWith(ctx, callback)
	if err != nil {
		return nil, err
	}

	// note: durability is ensured only if the store is in sync mode
	err = s.commitWHub.WaitFor(ctx, hdr.ID)
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return nil, ErrAlreadyClosed
	}
	if err != nil {
		return nil, err
	}

	if waitForIndexing {
		err = s.WaitForIndexingUpto(ctx, hdr.ID)

		// header is returned because transaction is already committed
		if err != nil {
			return hdr, err
		}
	}

	return hdr, nil
}

type KeyIndex interface {
	Get(ctx context.Context, key []byte) (valRef ValueRef, err error)
	GetBetween(ctx context.Context, key []byte, initialTxID, finalTxID uint64) (valRef ValueRef, err error)
	GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (valRef ValueRef, err error)
	GetWithPrefix(ctx context.Context, prefix []byte, neq []byte) (key []byte, valRef ValueRef, err error)
	GetWithPrefixAndFilters(ctx context.Context, prefix []byte, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error)
}

type unsafeIndex struct {
	st *ImmuStore
}

func (index *unsafeIndex) Get(ctx context.Context, key []byte) (ValueRef, error) {
	return index.GetWithFilters(ctx, key, IgnoreDeleted, IgnoreExpired)
}

func (index *unsafeIndex) GetBetween(ctx context.Context, key []byte, initialTxID, finalTxID uint64) (valRef ValueRef, err error) {
	return index.st.GetBetween(ctx, key, initialTxID, finalTxID)
}

func (index *unsafeIndex) GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (ValueRef, error) {
	return index.st.GetWithFilters(ctx, key, filters...)
}

func (index *unsafeIndex) GetWithPrefix(ctx context.Context, prefix []byte, neq []byte) (key []byte, valRef ValueRef, err error) {
	return index.st.GetWithPrefixAndFilters(ctx, prefix, neq, IgnoreDeleted, IgnoreExpired)
}

func (index *unsafeIndex) GetWithPrefixAndFilters(ctx context.Context, prefix []byte, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error) {
	return index.st.GetWithPrefixAndFilters(ctx, prefix, neq, filters...)
}

func (s *ImmuStore) preCommitWith(ctx context.Context, callback func(txID uint64, index KeyIndex) ([]*EntrySpec, []Precondition, error)) (*TxHeader, error) {
	if callback == nil {
		return nil, ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	otx, err := s.NewWriteOnlyTx(ctx)
	if err != nil {
		return nil, err
	}
	defer otx.Cancel()

	s.indexersMux.RLock()
	defer s.indexersMux.RUnlock()

	for _, indexer := range s.indexers {
		indexer.Pause()
		defer indexer.Resume()
	}

	lastPreCommittedTxID := s.LastPrecommittedTxID()

	otx.entries, otx.preconditions, err = callback(lastPreCommittedTxID+1, &unsafeIndex{st: s})
	if err != nil {
		return nil, err
	}

	if len(otx.entries) == 0 {
		return nil, ErrNoEntriesProvided
	}

	err = s.validateEntries(otx.entries)
	if err != nil {
		return nil, err
	}

	err = s.validatePreconditions(otx.preconditions)
	if err != nil {
		return nil, err
	}

	if otx.hasPreconditions() {
		for _, indexer := range s.indexers {
			indexer.Resume()
		}

		// Preconditions must be executed with up-to-date tree
		err = s.WaitForIndexingUpto(ctx, lastPreCommittedTxID)
		if err != nil {
			return nil, err
		}

		err = otx.checkPreconditions(ctx, s)
		if err != nil {
			return nil, err
		}

		for _, indexer := range s.indexers {
			indexer.Pause()
		}
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	tx.header.Version = s.writeTxHeaderVersion
	tx.header.NEntries = len(otx.entries)

	doneWithValuesCh := make(chan appendableResult)
	defer close(doneWithValuesCh)

	go func() {
		if s.embeddedValues {
			// value write is delayed to ensure values are inmediatelly followed by the associated tx header
			doneWithValuesCh <- appendableResult{nil, nil}
			return
		}

		offsets, err := s.appendValuesIntoAnyVLog(otx.entries)
		if err != nil {
			doneWithValuesCh <- appendableResult{nil, err}
			return
		}

		doneWithValuesCh <- appendableResult{offsets, nil}
	}()

	for i, e := range otx.entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.md = e.Metadata
		txe.vLen = len(e.Value)
		if e.IsValueTruncated {
			txe.hVal = e.HashValue
		} else {
			txe.hVal = sha256.Sum256(e.Value)
		}
	}

	err = tx.BuildHashTree()
	if err != nil {
		<-doneWithValuesCh // wait for data to be written
		return nil, err
	}

	valueWritingResult := <-doneWithValuesCh // wait for data to be written
	if valueWritingResult.err != nil {
		return nil, valueWritingResult.err
	}

	if !s.embeddedValues {
		for i := 0; i < tx.header.NEntries; i++ {
			tx.entries[i].vOff = valueWritingResult.offsets[i]
		}
	}

	err = s.performPrecommit(tx, otx.entries, s.timeFunc().Unix(), s.aht.Size())
	if err != nil {
		return nil, err
	}

	return tx.Header(), nil
}

type DualProofV2 struct {
	SourceTxHeader   *TxHeader
	TargetTxHeader   *TxHeader
	InclusionProof   [][sha256.Size]byte
	ConsistencyProof [][sha256.Size]byte
}

type DualProof struct {
	SourceTxHeader     *TxHeader
	TargetTxHeader     *TxHeader
	InclusionProof     [][sha256.Size]byte
	ConsistencyProof   [][sha256.Size]byte
	TargetBlTxAlh      [sha256.Size]byte
	LastInclusionProof [][sha256.Size]byte
	LinearProof        *LinearProof
	LinearAdvanceProof *LinearAdvanceProof
}

func (s *ImmuStore) DualProofV2(sourceTxHdr, targetTxHdr *TxHeader) (proof *DualProofV2, err error) {
	if sourceTxHdr == nil || targetTxHdr == nil || sourceTxHdr.ID == 0 {
		return nil, ErrIllegalArguments
	}

	if sourceTxHdr.ID > targetTxHdr.ID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	if sourceTxHdr.ID-1 != sourceTxHdr.BlTxID || targetTxHdr.ID-1 != targetTxHdr.BlTxID {
		return nil, ErrUnexpectedLinkingError
	}

	proof = &DualProofV2{
		SourceTxHeader: sourceTxHdr,
		TargetTxHeader: targetTxHdr,
	}

	if sourceTxHdr.ID < targetTxHdr.ID {
		proof.InclusionProof, err = s.aht.InclusionProof(sourceTxHdr.ID, targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}

		proof.ConsistencyProof, err = s.aht.ConsistencyProof(maxUint64(1, sourceTxHdr.BlTxID), targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}
	}

	return proof, nil
}

// DualProof combines linear cryptographic linking i.e. transactions include the linear accumulative hash up to the previous one,
// with binary cryptographic linking generated by appending the linear accumulative hash values into an incremental hash tree, whose
// root is also included as part of each transaction and thus considered when calculating the linear accumulative hash.
// The objective of this proof is the same as the linear proof, that is, generate data for the calculation of the accumulative
// hash value of the target transaction from the linear accumulative hash value up to source transaction.
func (s *ImmuStore) DualProof(sourceTxHdr, targetTxHdr *TxHeader) (proof *DualProof, err error) {
	if sourceTxHdr == nil || targetTxHdr == nil {
		return nil, ErrIllegalArguments
	}

	if sourceTxHdr.ID > targetTxHdr.ID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	proof = &DualProof{
		SourceTxHeader: sourceTxHdr,
		TargetTxHeader: targetTxHdr,
	}

	if sourceTxHdr.ID < targetTxHdr.BlTxID {
		binInclusionProof, err := s.aht.InclusionProof(sourceTxHdr.ID, targetTxHdr.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.InclusionProof = binInclusionProof
	}

	if sourceTxHdr.BlTxID > targetTxHdr.BlTxID {
		return nil, fmt.Errorf("%w: binary linking mismatch at tx %d", ErrCorruptedTxData, sourceTxHdr.ID)
	}

	if sourceTxHdr.BlTxID > 0 {
		// first root sourceTx.BlRoot, second one targetTx.BlRoot
		binConsistencyProof, err := s.aht.ConsistencyProof(sourceTxHdr.BlTxID, targetTxHdr.BlTxID)
		if err != nil {
			return nil, err
		}

		proof.ConsistencyProof = binConsistencyProof
	}

	if targetTxHdr.BlTxID > 0 {
		targetBlTxHdr, err := s.ReadTxHeader(targetTxHdr.BlTxID, false, false)
		if err != nil {
			return nil, err
		}

		proof.TargetBlTxAlh = targetBlTxHdr.Alh()

		// Used to validate targetTx.BlRoot is calculated with alh@targetTx.BlTxID as last leaf
		binLastInclusionProof, err := s.aht.InclusionProof(targetTxHdr.BlTxID, targetTxHdr.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.LastInclusionProof = binLastInclusionProof
	}

	lproof, err := s.LinearProof(maxUint64(sourceTxHdr.ID, targetTxHdr.BlTxID), targetTxHdr.ID)
	if err != nil {
		return nil, err
	}
	proof.LinearProof = lproof

	laproof, err := s.LinearAdvanceProof(
		sourceTxHdr.BlTxID,
		minUint64(sourceTxHdr.ID, targetTxHdr.BlTxID),
		targetTxHdr.BlTxID,
	)
	if err != nil {
		return nil, err
	}
	proof.LinearAdvanceProof = laproof

	return
}

type LinearProof struct {
	SourceTxID uint64
	TargetTxID uint64
	Terms      [][sha256.Size]byte
}

// LinearProof returns a list of hashes to calculate Alh@targetTxID from Alh@sourceTxID
func (s *ImmuStore) LinearProof(sourceTxID, targetTxID uint64) (*LinearProof, error) {
	if sourceTxID == 0 || sourceTxID > targetTxID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	r, err := s.NewTxReader(sourceTxID, false, tx)
	if err != nil {
		return nil, err
	}

	tx, err = r.Read()
	if err != nil {
		return nil, err
	}

	proof := make([][sha256.Size]byte, targetTxID-sourceTxID+1)
	proof[0] = tx.header.Alh()

	for i := 1; i < len(proof); i++ {
		tx, err := r.Read()
		if err != nil {
			return nil, err
		}

		proof[i] = tx.Header().innerHash()
	}

	return &LinearProof{
		SourceTxID: sourceTxID,
		TargetTxID: targetTxID,
		Terms:      proof,
	}, nil
}

// LinearAdvanceProof returns additional inclusion proof for part of the old linear proof consumed by
// the new Merkle Tree
func (s *ImmuStore) LinearAdvanceProof(sourceTxID, targetTxID uint64, targetBlTxID uint64) (*LinearAdvanceProof, error) {
	if targetTxID < sourceTxID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	if targetTxID <= sourceTxID+1 {
		return nil, nil // Additional proof is not needed
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	r, err := s.NewTxReader(sourceTxID+1, false, tx)
	if err != nil {
		return nil, err
	}

	tx, err = r.Read()
	if err != nil {
		return nil, err
	}

	linearProofTerms := make([][sha256.Size]byte, targetTxID-sourceTxID)
	linearProofTerms[0] = tx.header.Alh()

	inclusionProofs := make([][][sha256.Size]byte, targetTxID-sourceTxID-1)

	for txID := sourceTxID + 1; txID < targetTxID; txID++ {
		inclusionProof, err := s.aht.InclusionProof(txID, targetBlTxID)
		if err != nil {
			return nil, err
		}
		inclusionProofs[txID-sourceTxID-1] = inclusionProof

		tx, err := r.Read()
		if err != nil {
			return nil, err
		}
		linearProofTerms[txID-sourceTxID] = tx.Header().innerHash()
	}

	return &LinearAdvanceProof{
		LinearProofTerms: linearProofTerms,
		InclusionProofs:  inclusionProofs,
	}, nil
}

type LinearAdvanceProof struct {
	LinearProofTerms [][sha256.Size]byte
	InclusionProofs  [][][sha256.Size]byte
}

func (s *ImmuStore) txOffsetAndSize(txID uint64) (int64, int, error) {
	if txID == 0 {
		return 0, 0, ErrIllegalArguments
	}

	off := int64(txID-1) * int64(s.cLogEntrySize)

	cb := make([]byte, s.cLogEntrySize)

	_, err := s.cLog.ReadAt(cb[:], int64(off))
	if errors.Is(err, multiapp.ErrAlreadyClosed) || errors.Is(err, singleapp.ErrAlreadyClosed) {
		return 0, 0, ErrAlreadyClosed
	}

	// A partially readable commit record must be discarded -
	// - it is a result of incomplete commit-log write
	// and will be overwritten on the next commit
	if errors.Is(err, io.EOF) {
		return 0, 0, ErrTxNotFound
	}
	if err != nil {
		return 0, 0, err
	}

	txOffset := int64(binary.BigEndian.Uint64(cb))
	txSize := int(binary.BigEndian.Uint32(cb[offsetSize:]))

	return txOffset, txSize, nil
}

type slicedReaderAt struct {
	bs  []byte
	off int64
}

func (r *slicedReaderAt) ReadAt(bs []byte, off int64) (n int, err error) {
	if off < r.off || int(off-r.off) > len(bs) {
		return 0, ErrIllegalState
	}

	o := int(off - r.off)
	available := len(r.bs) - o

	copy(bs, r.bs[o:minInt(available, len(bs))])

	if len(bs) > available {
		return available, io.EOF
	}

	return available, nil
}

func (s *ImmuStore) ExportTx(txID uint64, allowPrecommitted bool, skipIntegrityCheck bool, tx *Tx) ([]byte, error) {
	err := s.readTx(txID, allowPrecommitted, skipIntegrityCheck, tx)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	hdrBs, err := tx.Header().Bytes()
	if err != nil {
		return nil, err
	}

	var b [lszSize]byte
	binary.BigEndian.PutUint32(b[:], uint32(len(hdrBs)))
	_, err = buf.Write(b[:])
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(hdrBs)
	if err != nil {
		return nil, err
	}

	var isValueTruncated bool

	for i, e := range tx.Entries() {
		var blen [lszSize]byte

		// kLen
		binary.BigEndian.PutUint16(blen[:], uint16(e.kLen))
		_, err = buf.Write(blen[:sszSize])
		if err != nil {
			return nil, err
		}

		// key
		_, err = buf.Write(e.Key())
		if err != nil {
			return nil, err
		}

		var md []byte

		if e.md != nil {
			md = e.md.Bytes()
		}

		// mdLen
		binary.BigEndian.PutUint16(blen[:], uint16(len(md)))
		_, err = buf.Write(blen[:sszSize])
		if err != nil {
			return nil, err
		}

		// md
		_, err = buf.Write(md)
		if err != nil {
			return nil, err
		}

		// val
		// TODO: improve value reading implementation, get rid of _valBs
		s._valBsMux.Lock()

		var valBuf []byte
		if e.vLen > len(s._valBs) {
			valBuf = make([]byte, e.vLen)
		} else {
			valBuf = s._valBs[:e.vLen]
		}

		_, err = s.readValueAt(valBuf, e.vOff, e.hVal, skipIntegrityCheck)
		if err != nil && !errors.Is(err, io.EOF) {
			s._valBsMux.Unlock()
			return nil, err
		}

		if err == nil {
			// currently, either all the values are sent or none
			if isValueTruncated {
				return nil, fmt.Errorf("%w: partially truncated transaction", ErrCorruptedData)
			}

			// vLen
			binary.BigEndian.PutUint32(blen[:], uint32(e.vLen))
			_, err = buf.Write(blen[:])
			if err != nil {
				s._valBsMux.Unlock()
				return nil, err
			}

			// val
			_, err = buf.Write(valBuf)
			if err != nil {
				s._valBsMux.Unlock()
				return nil, err
			}
		} else {
			// error is eof, the value has been truncated,
			// value is not available but digest is written instead

			// currently, either all the values are sent or none
			if !isValueTruncated && i > 0 {
				return nil, fmt.Errorf("%w: partially truncated transaction", ErrCorruptedData)
			}

			isValueTruncated = true

			// vHashLen
			binary.BigEndian.PutUint32(blen[:], uint32(len(e.hVal)))
			_, err = buf.Write(blen[:])
			if err != nil {
				s._valBsMux.Unlock()
				return nil, err
			}

			// vHash
			_, err = buf.Write(e.hVal[:])
			if err != nil {
				s._valBsMux.Unlock()
				return nil, err
			}
		}

		s._valBsMux.Unlock()
	}

	// NOTE: adding a boolean to the header to indicate if the transaction has values or not,
	// so that ReplicateTx knows if the transaction should be precommited with no values
	var truncatedValByte [1]byte
	truncatedValByte[0] = 0
	if isValueTruncated {
		truncatedValByte[0] = 1
	}

	binary.BigEndian.PutUint16(b[:], uint16(len(truncatedValByte)))
	_, err = buf.Write(b[:sszSize])
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(truncatedValByte[:])
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (s *ImmuStore) ReplicateTx(ctx context.Context, exportedTx []byte, skipIntegrityCheck bool, waitForIndexing bool) (*TxHeader, error) {
	if len(exportedTx) == 0 {
		return nil, ErrIllegalArguments
	}

	i := 0

	if len(exportedTx) < lszSize {
		return nil, ErrIllegalArguments
	}

	hdrLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
	i += lszSize

	if len(exportedTx) < i+hdrLen {
		return nil, ErrIllegalArguments
	}

	hdr := &TxHeader{}
	err := hdr.ReadFrom(exportedTx[i : i+hdrLen])
	if err != nil {
		return nil, err
	}
	i += hdrLen

	txSpec, err := s.NewWriteOnlyTx(ctx)
	if err != nil {
		return nil, err
	}

	txSpec.metadata = hdr.Metadata

	var entries []*EntrySpec = make([]*EntrySpec, 0)

	for e := 0; e < hdr.NEntries; e++ {
		if len(exportedTx) < i+2*sszSize+lszSize {
			return nil, ErrIllegalArguments
		}

		kLen := int(binary.BigEndian.Uint16(exportedTx[i:]))
		i += sszSize

		if len(exportedTx) < i+sszSize+lszSize+kLen {
			return nil, ErrIllegalArguments
		}

		key := make([]byte, kLen)
		copy(key, exportedTx[i:])
		i += kLen

		mdLen := int(binary.BigEndian.Uint16(exportedTx[i:]))
		i += sszSize

		if len(exportedTx) < i+mdLen {
			return nil, ErrIllegalArguments
		}

		var md *KVMetadata

		if mdLen > 0 {
			md = newReadOnlyKVMetadata()

			err := md.unsafeReadFrom(exportedTx[i : i+mdLen])
			if err != nil {
				return nil, err
			}
			i += mdLen
		}

		// value
		vLen := int(binary.BigEndian.Uint32(exportedTx[i:]))
		i += lszSize

		if len(exportedTx) < i+vLen {
			return nil, ErrIllegalArguments
		}

		entries = append(entries, &EntrySpec{
			Key:      key,
			Metadata: md,
			Value:    exportedTx[i : i+vLen],
		})

		i += vLen
	}

	var isTruncated bool

	// check if there is truncated value information in the transaction
	if i < len(exportedTx) {
		// information for truncated value
		tLen := int(binary.BigEndian.Uint16(exportedTx[i:]))
		i += sszSize
		if len(exportedTx) < i+tLen {
			return nil, ErrIllegalArguments
		}

		v := exportedTx[i : i+tLen]
		// v[0] == 1 means that the value is truncated
		// validate that the value is either 0 or 1
		if len(v) > 0 && v[0] > 1 {
			return nil, ErrIllegalTruncationArgument
		}
		isTruncated = v[0] == 1
		i += tLen
	}

	if i != len(exportedTx) {
		return nil, ErrIllegalArguments
	}

	// add entries to tx
	for _, e := range entries {
		var err error
		if isTruncated {
			err = txSpec.set(e.Key, e.Metadata, nil, digest(e.Value), isTruncated, false)
		} else {
			err = txSpec.set(e.Key, e.Metadata, e.Value, e.HashValue, isTruncated, false)
		}
		if err != nil {
			return nil, err
		}
	}

	txHdr, err := s.precommit(ctx, txSpec, hdr, skipIntegrityCheck)
	if err != nil {
		return nil, err
	}

	// wait for syncing to happen before exposing the header
	err = s.durablePrecommitWHub.WaitFor(ctx, txHdr.ID)
	if errors.Is(err, watchers.ErrAlreadyClosed) {
		return nil, ErrAlreadyClosed
	}
	if err != nil {
		return nil, err
	}

	s.commitStateRWMutex.Lock()
	waitForCommit := !s.useExternalCommitAllowance
	s.commitStateRWMutex.Unlock()

	if waitForCommit {
		err = s.commitWHub.WaitFor(ctx, txHdr.ID)
		if errors.Is(err, watchers.ErrAlreadyClosed) {
			return nil, ErrAlreadyClosed
		}
		if err != nil {
			return nil, err
		}

		if waitForIndexing {
			err = s.WaitForIndexingUpto(ctx, txHdr.ID)
			if err != nil {
				return txHdr, err
			}
		}
	}

	return txHdr, nil
}

func (s *ImmuStore) FirstTxSince(ts time.Time) (*TxHeader, error) {
	left := uint64(1)
	right := s.LastCommittedTxID()

	for left < right {
		middle := left + (right-left)/2

		header, err := s.ReadTxHeader(middle, false, false)
		if err != nil {
			return nil, err
		}

		if header.Ts < ts.Unix() {
			left = middle + 1
		} else {
			right = middle
		}
	}

	header, err := s.ReadTxHeader(left, false, false)
	if err != nil {
		return nil, err
	}

	if header.Ts < ts.Unix() {
		return nil, ErrTxNotFound
	}

	return header, nil
}

func (s *ImmuStore) LastTxUntil(ts time.Time) (*TxHeader, error) {
	left := uint64(1)
	right := s.LastCommittedTxID()

	for left < right {
		middle := left + ((right-left)+1)/2

		header, err := s.ReadTxHeader(middle, false, false)
		if err != nil {
			return nil, err
		}

		if header.Ts > ts.Unix() {
			right = middle - 1
		} else {
			left = middle
		}
	}

	header, err := s.ReadTxHeader(left, false, false)
	if err != nil {
		return nil, err
	}

	if header.Ts > ts.Unix() {
		return nil, ErrTxNotFound
	}

	return header, nil
}

func (s *ImmuStore) appendableReaderForTx(txID uint64, allowPrecommitted bool) (*appendable.Reader, error) {
	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	if txID > s.inmemPrecommittedTxID || (!allowPrecommitted && txID > s.committedTxID) {
		return nil, ErrTxNotFound
	}

	cacheMiss := false

	txbs, err := s.txLogCache.Get(txID)
	if err != nil {
		if errors.Is(err, cache.ErrKeyNotFound) {
			cacheMiss = true
		} else {
			return nil, err
		}
	}

	var txOff int64
	var txSize int

	if txID <= s.committedTxID {
		txOff, txSize, err = s.txOffsetAndSize(txID)
	} else {
		_, _, txOff, txSize, err = s.cLogBuf.readAhead(int(txID - s.committedTxID - 1))
	}
	if err != nil {
		return nil, err
	}

	var txr io.ReaderAt

	if cacheMiss {
		txr = s.txLog
	} else {
		txr = &slicedReaderAt{bs: txbs.([]byte), off: txOff}
	}
	return appendable.NewReaderFrom(txr, txOff, txSize), nil
}

func (s *ImmuStore) ReadTx(txID uint64, skipIntegrityCheck bool, tx *Tx) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	return s.readTx(txID, false, skipIntegrityCheck, tx)
}

func (s *ImmuStore) readTx(txID uint64, allowPrecommitted bool, skipIntegrityCheck bool, tx *Tx) error {
	r, err := s.appendableReaderForTx(txID, allowPrecommitted)
	if err != nil {
		return err
	}

	err = tx.readFrom(r, skipIntegrityCheck)
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: unexpected EOF while reading tx %d", ErrCorruptedTxData, txID)
	}

	return err
}

func (s *ImmuStore) ReadTxHeader(txID uint64, allowPrecommitted bool, skipIntegrityCheck bool) (*TxHeader, error) {
	r, err := s.appendableReaderForTx(txID, allowPrecommitted)
	if err != nil {
		return nil, err
	}

	tdr := &txDataReader{r: r, skipIntegrityCheck: skipIntegrityCheck}

	header, err := tdr.readHeader(s.maxTxEntries)
	if err != nil {
		return nil, err
	}

	e := &TxEntry{k: make([]byte, s.maxKeyLen)}

	for i := 0; i < header.NEntries; i++ {
		err = tdr.readEntry(e)
		if err != nil {
			return nil, err
		}
	}

	htree, err := htree.New(header.NEntries)
	if err != nil {
		return nil, err
	}

	err = tdr.buildAndValidateHtree(htree)
	if err != nil {
		return nil, err
	}

	return header, nil
}

func (s *ImmuStore) ReadTxEntry(txID uint64, key []byte, skipIntegrityCheck bool) (*TxEntry, *TxHeader, error) {
	var ret *TxEntry

	r, err := s.appendableReaderForTx(txID, false)
	if err != nil {
		return nil, nil, err
	}

	tdr := &txDataReader{r: r, skipIntegrityCheck: skipIntegrityCheck}

	header, err := tdr.readHeader(s.maxTxEntries)
	if err != nil {
		return nil, nil, err
	}

	e := &TxEntry{k: make([]byte, s.maxKeyLen)}

	for i := 0; i < header.NEntries; i++ {
		err = tdr.readEntry(e)
		if err != nil {
			return nil, nil, err
		}

		if bytes.Equal(e.key(), key) {
			if ret != nil {
				return nil, nil, ErrCorruptedTxDataDuplicateKey
			}
			ret = e

			// Allocate new placeholder for scanning the rest of entries
			e = &TxEntry{k: make([]byte, s.maxKeyLen)}
		}
	}
	if ret == nil {
		return nil, nil, ErrKeyNotFound
	}

	htree, err := htree.New(header.NEntries)
	if err != nil {
		return nil, nil, err
	}

	err = tdr.buildAndValidateHtree(htree)
	if err != nil {
		return nil, nil, err
	}

	return ret, header, nil
}

// ReadValue returns the actual associated value to a key at a specific transaction
// ErrExpiredEntry is be returned if the specified time has already elapsed
func (s *ImmuStore) ReadValue(entry *TxEntry) ([]byte, error) {
	if entry == nil || !entry.readonly {
		return nil, ErrIllegalArguments
	}

	if entry.md != nil && !entry.md.readonly {
		return nil, ErrIllegalArguments
	}

	if entry.md != nil && entry.md.ExpiredAt(time.Now()) {
		return nil, ErrExpiredEntry
	}

	if entry.vLen == 0 {
		// while not required, nil is returned instead of an empty slice

		// TODO: this step should be done after reading the value to ensure proper validations are made
		// But current changes in ExportTx with truncated transactions are not providing the value length
		// for truncated transactions, making it impossible to differentiate an empty value with a truncated one
		return nil, nil
	}

	b := make([]byte, entry.vLen)

	_, err := s.readValueAt(b, entry.vOff, entry.hVal, false)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// readValueAt fills b with the value referenced by off
// expected value size and digest may be required for validations to pass
func (s *ImmuStore) readValueAt(b []byte, off int64, hvalue [sha256.Size]byte, skipIntegrityCheck bool) (n int, err error) {
	vLogID, offset := decodeOffset(off)

	if !s.embeddedValues && vLogID == 0 && len(b) > 0 {
		return 0, io.EOF // it means value was not stored on any vlog i.e. a truncated transaction was replicated
	}

	if len(b) > 0 {
		foundInTheCache := false

		if s.vLogCache != nil {
			val, err := s.vLogCache.Get(off)
			if err == nil {
				bval := val.([]byte) // the requested value was found in the value cache
				copy(b, bval)
				n = len(bval)
				foundInTheCache = true
			} else if !errors.Is(err, cache.ErrKeyNotFound) {
				return 0, err
			}
		}

		if !foundInTheCache {
			vLog, err := s.fetchVLog(vLogID)
			if err != nil {
				return 0, err
			}
			defer s.releaseVLog(vLogID)

			n, err = vLog.ReadAt(b, offset)
			if errors.Is(err, multiapp.ErrAlreadyClosed) || errors.Is(err, singleapp.ErrAlreadyClosed) {
				return n, ErrAlreadyClosed
			}
			if err != nil {
				return n, err
			}

			if s.vLogCache != nil {
				cb := make([]byte, n)
				copy(cb, b)

				_, _, err = s.vLogCache.Put(off, cb)
				if err != nil {
					return n, err
				}
			}
		}
	}

	// either value was empty (n == 0)
	// or a non-empty value (n > 0) was read from cache or disk

	if !skipIntegrityCheck && (len(b) != n || hvalue != sha256.Sum256(b[:n])) {
		return n, fmt.Errorf("%w: value length or digest mismatch", ErrCorruptedData)
	}

	return n, nil
}

func (s *ImmuStore) validateEntries(entries []*EntrySpec) error {
	if len(entries) > s.maxTxEntries {
		return ErrMaxTxEntriesLimitExceeded
	}

	m := make(map[string]struct{}, len(entries))

	for _, kv := range entries {
		if kv.Key == nil {
			return ErrNullKey
		}

		if len(kv.Key) > s.maxKeyLen {
			return ErrMaxKeyLenExceeded
		}
		if len(kv.Value) > s.maxValueLen {
			return ErrMaxValueLenExceeded
		}

		ks := slices.BytesToString(kv.Key)
		if _, ok := m[ks]; ok {
			return ErrDuplicatedKey
		}
		m[ks] = struct{}{}
	}

	return nil
}

func (s *ImmuStore) validatePreconditions(preconditions []Precondition) error {
	if len(preconditions) > s.maxTxEntries {
		return ErrInvalidPreconditionTooMany
	}

	for _, c := range preconditions {
		if c == nil {
			return ErrInvalidPreconditionNull
		}

		err := c.Validate(s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ImmuStore) Sync() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}
	return s.sync()
}

func (s *ImmuStore) sync() error {
	s.commitStateRWMutex.Lock()
	defer s.commitStateRWMutex.Unlock()

	if s.inmemPrecommittedTxID == s.committedTxID {
		// everything already synced
		return nil
	}

	for i := range s.vLogs {
		vLog, err := s.fetchVLog(i + 1)
		if err != nil {
			return err
		}
		defer s.releaseVLog(i + 1)

		err = vLog.Flush()
		if err != nil {
			return err
		}

		err = vLog.Sync()
		if err != nil {
			return err
		}
	}

	err := s.txLog.Flush()
	if err != nil {
		return err
	}

	err = s.txLog.Sync()
	if err != nil {
		return err
	}

	err = s.durablePrecommitWHub.DoneUpto(s.inmemPrecommittedTxID)
	if err != nil {
		return err
	}

	commitAllowedUpToTxID := s.commitAllowedUpTo()
	txsCountToBeCommitted := int(commitAllowedUpToTxID - s.committedTxID)

	if txsCountToBeCommitted == 0 {
		return nil
	}

	// will overwrite partially written and uncommitted data
	err = s.cLog.SetOffset(int64(s.committedTxID) * int64(s.cLogEntrySize))
	if err != nil {
		return err
	}

	var commitUpToTxID uint64
	var commitUpToTxAlh [sha256.Size]byte

	for i := 0; i < txsCountToBeCommitted; i++ {
		txID, alh, txOff, txSize, err := s.cLogBuf.readAhead(i)
		if err != nil {
			return err
		}

		cb := make([]byte, s.cLogEntrySize)
		binary.BigEndian.PutUint64(cb, uint64(txOff))
		binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))

		if s.cLogEntrySize == cLogEntrySizeV2 {
			copy(cb[offsetSize+lszSize:], alh[:])
		}

		_, _, err = s.cLog.Append(cb)
		if err != nil {
			return err
		}

		commitUpToTxID = txID
		commitUpToTxAlh = alh
	}

	// added as a safety fuse but this situation should NOT happen
	if commitUpToTxID != commitAllowedUpToTxID {
		return fmt.Errorf("%w: may commit up to %d but actual transaction to be committed is %d", ErrUnexpectedError, commitAllowedUpToTxID, commitUpToTxID)
	}

	err = s.cLog.Flush()
	if err != nil {
		return err
	}

	err = s.cLog.Sync()
	if err != nil {
		return err
	}

	err = s.cLogBuf.advanceReader(txsCountToBeCommitted)
	if err != nil {
		return err
	}

	s.committedTxID = commitUpToTxID
	s.committedAlh = commitUpToTxAlh

	s.commitWHub.DoneUpto(commitUpToTxID)

	return nil
}

func (s *ImmuStore) IsClosed() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.closed
}

func (s *ImmuStore) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	s.closed = true

	merr := multierr.NewMultiErr()

	for _, indexer := range s.indexers {
		err := indexer.Close()
		merr.Append(err)
	}

	for i := range s.vLogs {
		vLog, err := s.fetchVLog(i + 1)
		merr.Append(err)

		err = vLog.Close()
		merr.Append(err)

		err = s.releaseVLog(i + 1)
		merr.Append(err)
	}

	err := s.inmemPrecommitWHub.Close()
	merr.Append(err)

	err = s.durablePrecommitWHub.Close()
	merr.Append(err)

	err = s.commitWHub.Close()
	merr.Append(err)

	err = s.txLog.Close()
	merr.Append(err)

	err = s.cLog.Close()
	merr.Append(err)

	err = s.aht.Close()
	merr.Append(err)

	used, _, _ := s.txPool.Stats()
	if used > 0 {
		merr.Append(errors.New("not all tx holders were released"))
	}

	return merr.Reduce()
}

func (s *ImmuStore) wrapAppendableErr(err error, action string) error {
	if errors.Is(err, singleapp.ErrAlreadyClosed) || errors.Is(err, multiapp.ErrAlreadyClosed) {
		s.logger.Warningf("Got '%v' while '%s'", err, action)
		return ErrAlreadyClosed
	}

	return err
}

func minInt(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a <= b {
		return b
	}
	return a
}

func minUint64(a, b uint64) uint64 {
	if a >= b {
		return b
	}
	return a
}

// readTxOffsetAt reads the value-log offset of a specific entry (index) in a transaction (txID)
// txID is the transaction ID
// index is the index of the entry in the transaction
// allowPrecommitted indicates if a precommitted transaction can be read
func (s *ImmuStore) readTxOffsetAt(txID uint64, allowPrecommitted bool, index int) (*TxEntry, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	r, err := s.appendableReaderForTx(txID, allowPrecommitted)
	if err != nil {
		return nil, err
	}

	tdr := &txDataReader{r: r}

	hdr, err := tdr.readHeader(s.maxTxEntries)
	if err != nil {
		return nil, err
	}

	if hdr.NEntries < index {
		return nil, ErrTxEntryIndexOutOfRange
	}

	e := &TxEntry{k: make([]byte, s.maxKeyLen)}

	for i := 0; i < index; i++ {
		err = tdr.readEntry(e)
		if err != nil {
			return nil, err
		}
	}

	return e, nil
}

// TruncateUptoTx deletes the value-log file up to transactions
// that are strictly below the specified minTxID.
func (s *ImmuStore) TruncateUptoTx(minTxID uint64) error {
	/*
		When values are appended to the value log file, they are not
		inserted in strict monotic order of time. Depending on the
		maxConcurrency value, there could be n transactions trying
		to insert into the log at the same time. This could lead to
		the situation where a transaction (n+1) could be inserted
		in the value log before transaction (n)
						  discard point
								|
								v
				--------+-------+--------+----------
						|       |        |
					tn+1:vx   tn:vx  tn-1:vx
						|                |
						+----------------+
						max concurrency
							range
		If the log is truncated upto tn, it could lead to removal of
		data for tn+1. To avoid this overlap, we first go back from
		the discard point to fetch offsets across vlogs for safe delete
		points, and then check for transactions further than the
		discard point to figure out the least offset from the range
		which can safely be deleted. This offset, tn+1 in the above
		scenario is the safest point to discard upto to avoid deletion
		of values for any future transaction.
	*/

	s.logger.Infof("running truncation up to transaction '%d'", minTxID)

	if s.embeddedValues {
		s.logger.Infof("truncation with embedded values does not delete any data")
		return nil
	}

	// tombstones maintain the minimum offset for each value log file that can be safely deleted.
	tombstones := make(map[byte]int64)

	readFirstEntryOffset := func(id uint64) (*TxEntry, error) {
		return s.readTxOffsetAt(id, false, 1)
	}

	back := func(txID uint64) error {
		firstTxEntry, err := readFirstEntryOffset(txID)
		if err != nil {
			return err
		}

		// Iterate over past transactions and store the minimum offset for each value log file.
		vLogID, off := decodeOffset(firstTxEntry.VOff())
		if _, ok := tombstones[vLogID]; !ok {
			tombstones[vLogID] = off
		}
		return nil
	}

	front := func(txID uint64) error {
		firstTxEntry, err := readFirstEntryOffset(txID)
		if err != nil {
			return err
		}

		// Check if any future transaction offset lies before past transaction(s)
		// If so, then update the offset to the minimum offset for that value log file.
		vLogID, off := decodeOffset(firstTxEntry.VOff())
		if val, ok := tombstones[vLogID]; ok {
			if off < val {
				tombstones[vLogID] = off
			}
		}
		return nil
	}

	// Walk back to get offsets across vlogs which can be deleted safely.
	// This way, we can calculate the minimum offset for each value log file.
	{
		var i uint64 = minTxID
		for i > 0 && len(tombstones) != s.MaxIOConcurrency() {
			err := back(i)
			// if there is an error reading a transaction, stop the traversal and return the error.
			if err != nil && !errors.Is(err, ErrTxEntryIndexOutOfRange) /* tx has entries*/ {
				s.logger.Errorf("failed to fetch transaction %d {traversal=back, err = %v}", i, err)
				return err
			}
			i--
		}
	}

	// Walk front to check if any future transaction offset lies before past transaction(s)
	{
		// Check for transactions upto the last committed transaction to avoid deletion of values for any future transaction.
		maxTxID := s.LastCommittedTxID()
		s.logger.Infof("running truncation check between transaction '%d' and '%d'", minTxID, maxTxID)

		// TODO: add more integration tests
		// Iterate over all future transactions to check if any offset lies before past transaction(s) offset.
		for j := minTxID; j <= maxTxID; j++ {
			err := front(j)
			if err != nil && !errors.Is(err, ErrTxEntryIndexOutOfRange) /* tx has entries*/ {
				s.logger.Errorf("failed to fetch transaction %d {traversal=front, err = %v}", j, err)
				return err
			}
		}
	}

	// Delete offset from different value logs
	merr := multierr.NewMultiErr()
	{
		for vLogID, offset := range tombstones {
			vlog, err := s.fetchVLog(vLogID)
			if err != nil {
				merr.Append(err)
				continue
			}
			defer s.releaseVLog(vLogID)

			s.logger.Infof("truncating vlog '%d' at offset '%d'", vLogID, offset)
			err = vlog.DiscardUpto(offset)
			merr.Append(err)
		}
	}

	return merr.Reduce()
}

func digest(s []byte) [sha256.Size]byte {
	var a [sha256.Size]byte
	copy(a[:], s)
	return a
}
