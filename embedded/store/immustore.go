/*
Copyright 2019-2020 vChain, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

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
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/appendable"
	"github.com/codenotary/immudb/embedded/appendable/multiapp"
	"github.com/codenotary/immudb/embedded/cbuffer"
	"github.com/codenotary/immudb/embedded/multierr"
	"github.com/codenotary/immudb/embedded/tbtree"
)

var ErrIllegalArguments = errors.New("illegal arguments")
var ErrAlreadyClosed = errors.New("already closed")
var ErrUnexpectedLinkingError = errors.New("Internal inconsistency between linear and binary linking")
var ErrorNoEntriesProvided = errors.New("no entries provided")
var ErrorMaxTxEntriesLimitExceeded = errors.New("max number of entries per tx exceeded")
var ErrNullKey = errors.New("null key")
var ErrorMaxKeyLenExceeded = errors.New("max key length exceeded")
var ErrorMaxValueLenExceeded = errors.New("max value length exceeded")
var ErrDuplicatedKey = errors.New("duplicated key")
var ErrMaxConcurrencyLimitExceeded = errors.New("max concurrency limit exceeded")
var ErrorPathIsNotADirectory = errors.New("path is not a directory")
var ErrorCorruptedTxData = errors.New("tx data is corrupted")
var ErrCorruptedData = errors.New("data is corrupted")
var ErrCorruptedCLog = errors.New("commit log is corrupted")
var ErrTxSizeGreaterThanMaxTxSize = errors.New("tx size greater than max tx size")
var ErrCorruptedAHtree = errors.New("appendable hash tree is corrupted")
var ErrKeyNotFound = tbtree.ErrKeyNotFound
var ErrTxNotFound = errors.New("tx not found")
var ErrNoMoreEntries = tbtree.ErrNoMoreEntries

var ErrSourceTxNewerThanTargetTx = errors.New("source tx is newer than target tx")
var ErrLinearProofMaxLenExceeded = errors.New("max linear proof length limit exceeded")

const MaxKeyLen = 1024 // assumed to be not lower than hash size

const MaxParallelIO = 127

const cLogEntrySize = offsetSize + szSize // tx offset & size

const txIDSize = 8
const tsSize = 8
const szSize = 4
const offsetSize = 8

const linkedLeafSize = txIDSize + tsSize + txIDSize + 3*sha256.Size

const Version = 1

const (
	metaVersion      = "VERSION"
	metaMaxTxEntries = "MAX_TX_ENTRIES"
	metaMaxKeyLen    = "MAX_KEY_LEN"
	metaMaxValueLen  = "MAX_VALUE_LEN"
	metaFileSize     = "FILE_SIZE"
)

const indexPath = "index"
const cleanIndexPath = "index_cleaning"
const ahtPath = "aht"

type ImmuStore struct {
	path string

	vLogs            map[byte]*refVLog
	vLogUnlockedList *list.List
	vLogsCond        *sync.Cond

	txLog appendable.Appendable
	cLog  appendable.Appendable

	committedTxID      uint64
	committedAlh       [sha256.Size]byte
	committedTxLogSize int64

	readOnly          bool
	synced            bool
	maxConcurrency    int
	maxIOConcurrency  int
	maxTxEntries      int
	maxKeyLen         int
	maxValueLen       int
	maxLinearProofLen int

	maxTxSize int

	_txs     *list.List // pre-allocated txs
	_txsLock sync.Mutex

	_txbs []byte // pre-allocated buffer to support tx serialization

	_kvs []*tbtree.KV //pre-allocated for indexing

	aht      *ahtree.AHtree
	blBuffer *cbuffer.CHBuffer
	blErr    error
	blCond   *sync.Cond

	index     *tbtree.TBtree
	indexCond *sync.Cond

	mutex sync.Mutex

	closed bool
}

type refVLog struct {
	vLog        appendable.Appendable
	unlockedRef *list.Element // unlockedRef == nil <-> vLog is locked
}

type KV struct {
	Key   []byte
	Value []byte
}

func (kv *KV) Digest() [sha256.Size]byte {
	b := make([]byte, len(kv.Key)+sha256.Size)

	copy(b[:], kv.Key)

	hvalue := sha256.Sum256(kv.Value)
	copy(b[len(kv.Key):], hvalue[:])

	return sha256.Sum256(b)
}

func Open(path string, opts *Options) (*ImmuStore, error) {
	if !validOptions(opts) {
		return nil, ErrIllegalArguments
	}

	finfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, opts.FileMode)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	} else if !finfo.IsDir() {
		return nil, ErrorPathIsNotADirectory
	}

	metadata := appendable.NewMetadata(nil)
	metadata.PutInt(metaVersion, Version)
	metadata.PutInt(metaMaxTxEntries, opts.MaxTxEntries)
	metadata.PutInt(metaMaxKeyLen, opts.MaxKeyLen)
	metadata.PutInt(metaMaxValueLen, opts.MaxValueLen)
	metadata.PutInt(metaFileSize, opts.FileSize)

	appendableOpts := multiapp.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithSynced(opts.Synced).
		WithFileSize(opts.FileSize).
		WithFileMode(opts.FileMode).
		WithMetadata(metadata.Bytes())

	vLogs := make([]appendable.Appendable, opts.MaxIOConcurrency)
	for i := 0; i < opts.MaxIOConcurrency; i++ {
		appendableOpts.WithFileExt("val")
		appendableOpts.WithCompressionFormat(opts.CompressionFormat)
		appendableOpts.WithCompresionLevel(opts.CompressionLevel)
		appendableOpts.WithMaxOpenedFiles(opts.VLogMaxOpenedFiles)
		vLogPath := filepath.Join(path, fmt.Sprintf("val_%d", i))
		vLog, err := multiapp.Open(vLogPath, appendableOpts)
		if err != nil {
			return nil, err
		}
		vLogs[i] = vLog
	}

	appendableOpts.WithFileExt("tx")
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.TxLogMaxOpenedFiles)
	txLogPath := filepath.Join(path, "tx")
	txLog, err := multiapp.Open(txLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	appendableOpts.WithFileExt("txi")
	appendableOpts.WithCompressionFormat(appendable.NoCompression)
	appendableOpts.WithMaxOpenedFiles(opts.CommitLogMaxOpenedFiles)
	cLogPath := filepath.Join(path, "commit")
	cLog, err := multiapp.Open(cLogPath, appendableOpts)
	if err != nil {
		return nil, err
	}

	return OpenWith(path, vLogs, txLog, cLog, opts)
}

func OpenWith(path string, vLogs []appendable.Appendable, txLog, cLog appendable.Appendable, opts *Options) (*ImmuStore, error) {
	if !validOptions(opts) || len(vLogs) == 0 || txLog == nil || cLog == nil {
		return nil, ErrIllegalArguments
	}

	metadata := appendable.NewMetadata(cLog.Metadata())

	fileSize, ok := metadata.GetInt(metaFileSize)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	maxTxEntries, ok := metadata.GetInt(metaMaxTxEntries)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	maxKeyLen, ok := metadata.GetInt(metaMaxKeyLen)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	maxValueLen, ok := metadata.GetInt(metaMaxValueLen)
	if !ok {
		return nil, ErrCorruptedCLog
	}

	cLogSize, err := cLog.Size()
	if err != nil {
		return nil, err
	}

	if cLogSize%cLogEntrySize > 0 {
		return nil, ErrCorruptedCLog
	}

	var committedTxLogSize int64
	var committedTxOffset int64
	var committedTxSize int

	var committedTxID uint64

	if cLogSize > 0 {
		b := make([]byte, cLogEntrySize)
		_, err := cLog.ReadAt(b, cLogSize-cLogEntrySize)
		if err != nil {
			return nil, err
		}
		committedTxOffset = int64(binary.BigEndian.Uint64(b))
		committedTxSize = int(binary.BigEndian.Uint32(b[txIDSize:]))
		committedTxLogSize = committedTxOffset + int64(committedTxSize)
		committedTxID = uint64(cLogSize) / cLogEntrySize
	}

	txLogFileSize, err := txLog.Size()
	if err != nil {
		return nil, err
	}

	if txLogFileSize < committedTxLogSize {
		return nil, ErrorCorruptedTxData
	}

	maxTxSize := maxTxSize(maxTxEntries, maxKeyLen)

	txs := list.New()

	for i := 0; i < opts.MaxConcurrency; i++ {
		tx := NewTx(maxTxEntries, maxKeyLen)
		txs.PushBack(tx)
	}

	// Extra tx pre-allocation for indexing thread
	txs.PushBack(NewTx(maxTxEntries, maxKeyLen))

	txbs := make([]byte, maxTxSize)

	committedAlh := sha256.Sum256(nil)

	if cLogSize > 0 {
		txReader := appendable.NewReaderFrom(txLog, committedTxOffset, committedTxSize)

		tx := txs.Front().Value.(*Tx)
		err = tx.readFrom(txReader)
		if err != nil {
			return nil, err
		}

		committedAlh = tx.Alh
	}

	vLogsMap := make(map[byte]*refVLog, len(vLogs))
	vLogUnlockedList := list.New()

	for i, vLog := range vLogs {
		e := vLogUnlockedList.PushBack(byte(i))
		vLogsMap[byte(i)] = &refVLog{vLog: vLog, unlockedRef: e}
	}

	indexPath := filepath.Join(path, indexPath)

	indexOpts := tbtree.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithFileSize(fileSize).
		WithSynced(false). // index is built from derived data
		WithCacheSize(opts.IndexOpts.CacheSize).
		WithFlushThld(opts.IndexOpts.FlushThld).
		WithMaxActiveSnapshots(opts.IndexOpts.MaxActiveSnapshots).
		WithMaxNodeSize(opts.IndexOpts.MaxNodeSize).
		WithRenewSnapRootAfter(opts.IndexOpts.RenewSnapRootAfter)

	index, err := tbtree.Open(indexPath, indexOpts)
	if err != nil {
		return nil, err
	}

	ahtPath := filepath.Join(path, ahtPath)

	ahtOpts := ahtree.DefaultOptions().
		WithReadOnly(opts.ReadOnly).
		WithFileMode(opts.FileMode).
		WithFileSize(fileSize).
		WithSynced(false) // built from derived data

	aht, err := ahtree.Open(ahtPath, ahtOpts)
	if err != nil {
		return nil, err
	}

	kvs := make([]*tbtree.KV, maxTxEntries)
	for i := range kvs {
		kvs[i] = &tbtree.KV{K: make([]byte, maxKeyLen), V: make([]byte, sha256.Size+szSize+offsetSize)}
	}

	var blBuffer *cbuffer.CHBuffer
	if opts.MaxLinearProofLen > 0 {
		blBuffer = cbuffer.New(opts.MaxLinearProofLen)
	}

	store := &ImmuStore{
		path:               path,
		txLog:              txLog,
		vLogs:              vLogsMap,
		vLogUnlockedList:   vLogUnlockedList,
		vLogsCond:          sync.NewCond(&sync.Mutex{}),
		cLog:               cLog,
		committedTxLogSize: committedTxLogSize,
		committedTxID:      committedTxID,
		committedAlh:       committedAlh,

		readOnly:          opts.ReadOnly,
		synced:            opts.Synced,
		maxConcurrency:    opts.MaxConcurrency,
		maxIOConcurrency:  opts.MaxIOConcurrency,
		maxTxEntries:      maxTxEntries,
		maxKeyLen:         maxKeyLen,
		maxValueLen:       maxValueLen,
		maxLinearProofLen: opts.MaxLinearProofLen,

		maxTxSize: maxTxSize,

		index:     index,
		indexCond: sync.NewCond(&sync.Mutex{}),

		aht:      aht,
		blBuffer: blBuffer,
		blCond:   sync.NewCond(&sync.Mutex{}),

		_kvs:  kvs,
		_txs:  txs,
		_txbs: txbs,
	}

	if store.aht.Size() > store.committedTxID {
		return nil, ErrCorruptedCLog
	}

	err = store.syncBinaryLinking()
	if err != nil {
		return nil, err
	}

	if store.blBuffer != nil {
		go store.binaryLinking()
	}

	go store.indexer()

	return store, nil
}

func (s *ImmuStore) Get(key []byte) (value []byte, tx uint64, hc uint64, err error) {
	return s.index.Get(key)
}

func (s *ImmuStore) GetTs(key []byte, offset uint64, descOrder bool, limit int) (txs []uint64, err error) {
	return s.index.GetTs(key, offset, descOrder, limit)
}

func (s *ImmuStore) NewTx() *Tx {
	return NewTx(s.maxTxEntries, s.maxKeyLen)
}

func (s *ImmuStore) Snapshot() (*tbtree.Snapshot, error) {
	return s.index.Snapshot()
}

func (s *ImmuStore) SnapshotSince(tx uint64) (*tbtree.Snapshot, error) {
	return s.index.SnapshotSince(tx)
}

func (s *ImmuStore) binaryLinking() {
	for {
		// TODO (jeroiraz): blBuffer+blCond may be replaced by a buffered channel
		s.blCond.L.Lock()

		if s.blBuffer.IsEmpty() {
			s.blCond.Wait()
		}

		alh, err := s.blBuffer.Get()

		s.blCond.L.Unlock()

		if err == nil {
			_, _, err = s.aht.Append(alh[:])
			if err == ErrAlreadyClosed {
				return
			}
		}

		if err != nil {
			s.SetBlErr(err)
			return
		}
	}
}

func (s *ImmuStore) SetBlErr(err error) {
	s.blCond.L.Lock()
	defer s.blCond.L.Unlock()

	s.blErr = err
}

func (s *ImmuStore) Alh() (uint64, [sha256.Size]byte) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.committedTxID, s.committedAlh
}

func (s *ImmuStore) BlInfo() (uint64, error) {
	s.blCond.L.Lock()
	defer s.blCond.L.Unlock()

	return s.aht.Size(), s.blErr
}

func (s *ImmuStore) syncBinaryLinking() error {
	if s.aht.Size() == s.committedTxID {
		return nil
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return err
	}
	defer s.releaseAllocTx(tx)

	txReader, err := s.NewTxReader(s.aht.Size()+1, false, tx)
	if err == ErrNoMoreEntries {
		return nil
	}
	if err != nil {
		return err
	}

	for {
		tx, err := txReader.Read()
		if err == ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		alh := tx.Alh
		s.aht.Append(alh[:])
	}

	return nil
}

func (s *ImmuStore) indexer() {
	for {
		err := s.doIndexing()
		if err != nil && err != ErrNoMoreEntries {
			return
		}
	}
}

func (s *ImmuStore) CleanIndex() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	indexPath := filepath.Join(s.path, indexPath)
	cleanIndexPath := filepath.Join(s.path, cleanIndexPath)
	defer os.RemoveAll(cleanIndexPath)

	err := s.index.DumpTo(cleanIndexPath, false)
	if err != nil {
		return err
	}

	err = s.index.Close()
	if err != nil {
		return err
	}

	err = os.RemoveAll(indexPath)
	if err != nil {
		return err
	}

	err = os.Rename(cleanIndexPath, indexPath)
	if err != nil {
		return err
	}

	s.index, err = tbtree.Open(indexPath, s.index.GetOptions())
	if err != nil {
		return err
	}

	return nil
}

func (s *ImmuStore) IndexInfo() uint64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.index.Ts()
}

func (s *ImmuStore) doIndexing() error {
	s.indexCond.L.Lock()

	if s.index.Ts() == s.TxCount() {
		s.indexCond.Wait()
	}

	s.indexCond.L.Unlock()

	txID := s.index.Ts() + 1

	tx, err := s.fetchAllocTx()
	if err != nil {
		return err
	}
	defer s.releaseAllocTx(tx)

	txReader, err := s.NewTxReader(txID, false, tx)
	if err == ErrNoMoreEntries {
		return nil
	}
	if err != nil {
		return err
	}

	for {
		tx, err := txReader.Read()
		if err == ErrNoMoreEntries {
			break
		}
		if err != nil {
			return err
		}

		txEntries := tx.Entries()

		for i, e := range txEntries {
			var b [szSize + offsetSize + sha256.Size]byte
			binary.BigEndian.PutUint32(b[:], uint32(e.vLen))
			binary.BigEndian.PutUint64(b[szSize:], uint64(e.vOff))
			copy(b[szSize+offsetSize:], e.hVal[:])

			s._kvs[i].K = e.key()
			s._kvs[i].V = b[:]
		}

		err = s.index.BulkInsert(s._kvs[:len(txEntries)])
		if err != nil {
			return err
		}
	}

	return err
}

func maxTxSize(maxTxEntries, maxKeyLen int) int {
	return txIDSize /*txID*/ +
		tsSize /*ts*/ +
		txIDSize /*blTxID*/ +
		sha256.Size /*blRoot*/ +
		sha256.Size /*prevAlh*/ +
		szSize /*|entries|*/ +
		maxTxEntries*(szSize /*kLen*/ +maxKeyLen /*key*/ +szSize /*vLen*/ +offsetSize /*vOff*/ +sha256.Size /*hValue*/) +
		sha256.Size /*eH*/ +
		sha256.Size /*txH*/
}

func (s *ImmuStore) ReadOnly() bool {
	return s.readOnly
}

func (s *ImmuStore) Synced() bool {
	return s.synced
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

func (s *ImmuStore) MaxLinearProofLen() int {
	return s.maxLinearProofLen
}

func (s *ImmuStore) TxCount() uint64 {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.committedTxID
}

func (s *ImmuStore) fetchAllocTx() (*Tx, error) {
	s._txsLock.Lock()
	defer s._txsLock.Unlock()

	if s._txs.Len() == 0 {
		return nil, ErrMaxConcurrencyLimitExceeded
	}

	return s._txs.Remove(s._txs.Front()).(*Tx), nil
}

func (s *ImmuStore) releaseAllocTx(tx *Tx) {
	s._txsLock.Lock()
	defer s._txsLock.Unlock()

	s._txs.PushBack(tx)
}

func encodeOffset(offset int64, vLogID byte) int64 {
	return int64(vLogID)<<56 | offset
}

func decodeOffset(offset int64) (byte, int64) {
	return byte(offset >> 56), offset & ^(0xff << 55)
}

func (s *ImmuStore) fetchAnyVLog() (vLodID byte, vLog appendable.Appendable, err error) {
	s.vLogsCond.L.Lock()

	for s.vLogUnlockedList.Len() == 0 {
		s.mutex.Lock()
		if s.closed {
			err = ErrAlreadyClosed
		}
		s.mutex.Unlock()

		if err != nil {
			return 0, nil, err
		}

		s.vLogsCond.Wait()
	}

	vLogID := s.vLogUnlockedList.Remove(s.vLogUnlockedList.Front()).(byte) + 1

	s.vLogs[vLogID-1].unlockedRef = nil // locked

	s.vLogsCond.L.Unlock()

	return vLogID, s.vLogs[vLogID-1].vLog, nil
}

func (s *ImmuStore) fetchVLog(vLogID byte, checkClosed bool) (vLog appendable.Appendable, err error) {
	s.vLogsCond.L.Lock()

	for s.vLogs[vLogID-1].unlockedRef == nil {
		if checkClosed {
			s.mutex.Lock()
			if s.closed {
				err = ErrAlreadyClosed
			}
			s.mutex.Unlock()
		}

		if err != nil {
			return nil, err
		}

		s.vLogsCond.Wait()
	}

	s.vLogUnlockedList.Remove(s.vLogs[vLogID-1].unlockedRef)
	s.vLogs[vLogID-1].unlockedRef = nil // locked

	s.vLogsCond.L.Unlock()

	return s.vLogs[vLogID-1].vLog, nil
}

func (s *ImmuStore) releaseVLog(vLogID byte) {
	s.vLogsCond.L.Lock()
	s.vLogs[vLogID-1].unlockedRef = s.vLogUnlockedList.PushBack(vLogID - 1) // unlocked
	s.vLogsCond.L.Unlock()
	s.vLogsCond.Signal()
}

type appendableResult struct {
	offsets []int64
	err     error
}

func (s *ImmuStore) appendData(entries []*KV, donec chan<- appendableResult) {
	offsets := make([]int64, len(entries))

	vLogID, vLog, err := s.fetchAnyVLog()
	if err != nil {
		donec <- appendableResult{nil, err}
		return
	}

	defer s.releaseVLog(vLogID)

	for i := 0; i < len(offsets); i++ {
		if len(entries[i].Value) == 0 {
			continue
		}

		voff, _, err := vLog.Append(entries[i].Value)
		if err != nil {
			donec <- appendableResult{nil, err}
			return
		}
		offsets[i] = encodeOffset(voff, vLogID)
	}

	err = vLog.Flush()
	if err != nil {
		donec <- appendableResult{nil, err}
	}

	donec <- appendableResult{offsets, nil}
}

func (s *ImmuStore) Commit(entries []*KV) (*TxMetadata, error) {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return nil, ErrAlreadyClosed
	}
	s.mutex.Unlock()

	err := s.validateEntries(entries)
	if err != nil {
		return nil, err
	}

	appendableCh := make(chan appendableResult)
	go s.appendData(entries, appendableCh)

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	tx.nentries = len(entries)

	for i, e := range entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.vLen = len(e.Value)
		txe.hVal = sha256.Sum256(e.Value)
	}

	tx.BuildHashTree()

	r := <-appendableCh // wait for data to be written
	err = r.err
	if err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	err = s.commit(tx, r.offsets)
	if err != nil {
		return nil, err
	}

	return tx.Metadata(), nil
}

func (s *ImmuStore) commit(tx *Tx, offsets []int64) error {
	// will overrite partially written and uncommitted data
	s.txLog.SetOffset(s.committedTxLogSize)

	tx.ID = s.committedTxID + 1
	tx.Ts = time.Now().Unix()

	blTxID, blRoot, err := s.aht.Root()
	if err != nil && err != ahtree.ErrEmptyTree {
		return err
	}

	tx.BlTxID = blTxID
	tx.BlRoot = blRoot

	if tx.ID <= tx.BlTxID {
		return ErrUnexpectedLinkingError
	}

	tx.PrevAlh = s.committedAlh

	txSize := 0

	// tx serialization into pre-allocated buffer
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.ID))
	txSize += txIDSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.Ts))
	txSize += tsSize
	binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(tx.BlTxID))
	txSize += txIDSize
	copy(s._txbs[txSize:], tx.BlRoot[:])
	txSize += sha256.Size
	copy(s._txbs[txSize:], tx.PrevAlh[:])
	txSize += sha256.Size
	binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(tx.nentries))
	txSize += szSize

	for i := 0; i < tx.nentries; i++ {
		e := tx.entries[i]

		txe := tx.entries[i]
		txe.vOff = offsets[i]

		// tx serialization using pre-allocated buffer
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(e.kLen))
		txSize += szSize
		copy(s._txbs[txSize:], e.k[:e.kLen])
		txSize += e.kLen
		binary.BigEndian.PutUint32(s._txbs[txSize:], uint32(e.vLen))
		txSize += szSize
		binary.BigEndian.PutUint64(s._txbs[txSize:], uint64(txe.vOff))
		txSize += offsetSize
		copy(s._txbs[txSize:], txe.hVal[:])
		txSize += sha256.Size
	}

	tx.CalcAlh()

	// tx serialization using pre-allocated buffer
	copy(s._txbs[txSize:], tx.Alh[:])
	txSize += sha256.Size

	txOff, _, err := s.txLog.Append(s._txbs[:txSize])
	if err != nil {
		return err
	}

	err = s.txLog.Flush()
	if err != nil {
		return err
	}

	if s.blBuffer == nil {
		_, _, err := s.aht.Append(tx.Alh[:])
		if err != nil {
			return err
		}
	} else {
		s.blCond.L.Lock()
		defer s.blCond.L.Unlock()

		err = s.blBuffer.Put(tx.Alh)
		if err != nil {
			if err == cbuffer.ErrBufferIsFull {
				return ErrLinearProofMaxLenExceeded
			}
			return err
		}
		s.blCond.Broadcast()
	}

	var cb [cLogEntrySize]byte
	binary.BigEndian.PutUint64(cb[:], uint64(txOff))
	binary.BigEndian.PutUint32(cb[offsetSize:], uint32(txSize))
	_, _, err = s.cLog.Append(cb[:])
	if err != nil {
		return err
	}

	err = s.cLog.Flush()
	if err != nil {
		return err
	}

	s.committedTxID++
	s.committedAlh = tx.Alh
	s.committedTxLogSize += int64(txSize)

	s.indexCond.Broadcast()

	return nil
}

func (s *ImmuStore) CommitWith(callback func(txID uint64) ([]*KV, error)) (*TxMetadata, error) {
	if callback == nil {
		return nil, ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	txID := s.committedTxID + 1

	entries, err := callback(txID)
	if err != nil {
		return nil, err
	}

	err = s.validateEntries(entries)
	if err != nil {
		return nil, err
	}

	appendableCh := make(chan appendableResult)
	go s.appendData(entries, appendableCh)

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	tx.nentries = len(entries)

	for i, e := range entries {
		txe := tx.entries[i]
		txe.setKey(e.Key)
		txe.vLen = len(e.Value)
		txe.hVal = sha256.Sum256(e.Value)
	}

	tx.BuildHashTree()

	r := <-appendableCh // wait for data to be writen
	err = r.err
	if err != nil {
		return nil, err
	}

	err = s.commit(tx, r.offsets)
	if err != nil {
		return nil, err
	}

	return tx.Metadata(), nil
}

type DualProof struct {
	SourceTxMetadata   *TxMetadata
	TargetTxMetadata   *TxMetadata
	InclusionProof     [][sha256.Size]byte
	ConsistencyProof   [][sha256.Size]byte
	TargetBlTxAlh      [sha256.Size]byte
	LastInclusionProof [][sha256.Size]byte
	LinearProof        *LinearProof
}

// DualProof combines linear cryptographic linking i.e. transactions include the linear accumulative hash up to the previous one,
// with binary cryptographic linking generated by appending the linear accumulative hash values into an incremental hash tree, whose
// root is also included as part of each transaction and thus considered when calculating the linear accumulative hash.
// The objective of this proof is the same as the linear proof, that is, generate data for the calculation of the accumulative
// hash value of the target transaction from the linear accumulative hash value up to source transaction.
func (s *ImmuStore) DualProof(sourceTx, targetTx *Tx) (proof *DualProof, err error) {
	if sourceTx == nil || targetTx == nil {
		return nil, ErrIllegalArguments
	}

	if sourceTx.ID > targetTx.ID {
		return nil, ErrSourceTxNewerThanTargetTx
	}

	proof = &DualProof{
		SourceTxMetadata: sourceTx.Metadata(),
		TargetTxMetadata: targetTx.Metadata(),
	}

	if sourceTx.ID < targetTx.BlTxID {
		binInclusionProof, err := s.aht.InclusionProof(sourceTx.ID, targetTx.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.InclusionProof = binInclusionProof
	}

	if sourceTx.BlTxID > targetTx.BlTxID {
		return nil, ErrorCorruptedTxData
	}

	if sourceTx.BlTxID > 0 {
		binConsistencyProof, err := s.aht.ConsistencyProof(sourceTx.BlTxID, targetTx.BlTxID) // first root sourceTx.BlRoot, second one targetTx.BlRoot
		if err != nil {
			return nil, err
		}

		proof.ConsistencyProof = binConsistencyProof
	}

	var targetBlTx *Tx

	if targetTx.BlTxID > 0 {
		targetBlTx, err = s.fetchAllocTx()
		if err != nil {
			return nil, err
		}

		err = s.ReadTx(targetTx.BlTxID, targetBlTx)
		if err != nil {
			return nil, err
		}

		proof.TargetBlTxAlh = targetBlTx.Alh

		// Used to validate targetTx.BlRoot is calculated with alh@targetTx.BlTxID as last leaf
		binLastInclusionProof, err := s.aht.InclusionProof(targetTx.BlTxID, targetTx.BlTxID) // must match targetTx.BlRoot
		if err != nil {
			return nil, err
		}
		proof.LastInclusionProof = binLastInclusionProof
	}

	if targetBlTx != nil {
		s.releaseAllocTx(targetBlTx)
	}

	lproof, err := s.LinearProof(maxUint64(sourceTx.ID, targetTx.BlTxID), targetTx.ID)
	if err != nil {
		return nil, err
	}
	proof.LinearProof = lproof

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

	if s.maxLinearProofLen > 0 && int(targetTxID-sourceTxID+1) > s.maxLinearProofLen {
		return nil, ErrLinearProofMaxLenExceeded
	}

	tx, err := s.fetchAllocTx()
	if err != nil {
		return nil, err
	}
	defer s.releaseAllocTx(tx)

	r, err := s.NewTxReader(sourceTxID, false, tx)

	tx, err = r.Read()
	if err != nil {
		return nil, err
	}

	proof := make([][sha256.Size]byte, targetTxID-sourceTxID+1)
	proof[0] = tx.Alh

	for i := 1; i < len(proof); i++ {
		tx, err := r.Read()
		if err != nil {
			return nil, err
		}

		proof[i] = tx.InnerHash
	}

	return &LinearProof{
		SourceTxID: sourceTxID,
		TargetTxID: targetTxID,
		Terms:      proof,
	}, nil
}

func (s *ImmuStore) txOffsetAndSize(txID uint64) (int64, int, error) {
	if txID == 0 {
		return 0, 0, ErrIllegalArguments
	}

	off := (txID - 1) * cLogEntrySize

	var cb [cLogEntrySize]byte

	n, err := s.cLog.ReadAt(cb[:], int64(off))
	if err == io.EOF && n == 0 {
		return 0, 0, ErrTxNotFound
	}
	if err == io.EOF && n > 0 {
		return 0, n, ErrCorruptedCLog
	}
	if err != nil {
		return 0, 0, err
	}

	txOffset := int64(binary.BigEndian.Uint64(cb[:]))
	txSize := int(binary.BigEndian.Uint32(cb[offsetSize:]))

	if txOffset > s.committedTxLogSize || txSize > s.maxTxSize {
		return 0, 0, ErrorCorruptedTxData
	}

	return txOffset, txSize, nil
}

func (s *ImmuStore) ReadTx(txID uint64, tx *Tx) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	txOff, txSize, err := s.txOffsetAndSize(txID)
	if err != nil {
		return err
	}

	r := appendable.NewReaderFrom(s.txLog, txOff, txSize)

	return tx.readFrom(r)
}

// ReadTxUnsafe should be used with care, inside Commit callback is safe
func (s *ImmuStore) ReadTxUnsafe(txID uint64, tx *Tx) error {
	txOff, txSize, err := s.txOffsetAndSize(txID)
	if err != nil {
		return err
	}

	r := appendable.NewReaderFrom(s.txLog, txOff, txSize)

	return tx.readFrom(r)
}

func (s *ImmuStore) ReadValue(tx *Tx, key []byte) ([]byte, error) {
	for _, e := range tx.Entries() {
		if bytes.Equal(e.key(), key) {
			v := make([]byte, e.vLen)
			_, err := s.ReadValueAt(v, e.vOff, e.hVal)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
	}
	return nil, ErrKeyNotFound
}

func (s *ImmuStore) ReadValueAt(b []byte, off int64, hvalue [sha256.Size]byte) (int, error) {
	vLogID, offset := decodeOffset(off)

	if vLogID > 0 {
		vLog, err := s.fetchVLog(vLogID, true)
		if err != nil {
			return 0, err
		}
		defer s.releaseVLog(vLogID)

		n, err := vLog.ReadAt(b, offset)
		if err != nil {
			return n, err
		}
	}

	if hvalue != sha256.Sum256(b) {
		return len(b), ErrCorruptedData
	}

	return len(b), nil
}

func (s *ImmuStore) validateEntries(entries []*KV) error {
	if len(entries) == 0 {
		return ErrorNoEntriesProvided
	}
	if len(entries) > s.maxTxEntries {
		return ErrorMaxTxEntriesLimitExceeded
	}

	m := make(map[string]struct{}, len(entries))

	for _, kv := range entries {
		if kv.Key == nil {
			return ErrNullKey
		}

		if len(kv.Key) > s.maxKeyLen {
			return ErrorMaxKeyLenExceeded
		}
		if len(kv.Value) > s.maxValueLen {
			return ErrorMaxValueLenExceeded
		}

		b64k := base64.StdEncoding.EncodeToString(kv.Key)
		if _, ok := m[b64k]; ok {
			return ErrDuplicatedKey
		}
		m[b64k] = struct{}{}
	}
	return nil
}

func (s *ImmuStore) Sync() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	for i := range s.vLogs {
		vLog, _ := s.fetchVLog(i+1, false)
		err := vLog.Sync()
		if err != nil {
			return err
		}
		s.releaseVLog(i + 1)
	}

	err := s.txLog.Sync()
	if err != nil {
		return err
	}

	err = s.cLog.Sync()
	if err != nil {
		return err
	}

	return s.index.Sync()
}

func (s *ImmuStore) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	s.closed = true

	errors := make([]error, 0)

	for i := range s.vLogs {
		vLog, _ := s.fetchVLog(i+1, false)

		err := vLog.Close()
		if err != nil {
			errors = append(errors, err)
		}
	}
	s.vLogsCond.Broadcast()

	txErr := s.txLog.Close()
	if txErr != nil {
		errors = append(errors, txErr)
	}

	cErr := s.cLog.Close()
	if cErr != nil {
		errors = append(errors, cErr)
	}

	tErr := s.aht.Close()
	if tErr != nil {
		errors = append(errors, tErr)
	}

	iErr := s.index.Close()
	if iErr != nil {
		errors = append(errors, iErr)
	}

	if len(errors) > 0 {
		return &multierr.MultiErr{Errors: errors}
	}

	return nil
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
