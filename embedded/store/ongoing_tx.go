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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"
)

// OngoingTx (no-thread safe) represents an interactive or incremental transaction with support of RYOW.
// The snapshot may be locally modified but isolated from other transactions
type OngoingTx struct {
	st *ImmuStore

	ctx context.Context

	snapshots []*Snapshot // snapshot per index

	mode                    TxMode // MVCC validations are not needed for read-only transactions
	snapshotMustIncludeTxID func(lastPrecommittedTxID uint64) uint64
	snapshotRenewalPeriod   time.Duration

	unsafeMVCC bool

	requireMVCCOnFollowingTxs bool

	entries          []*EntrySpec
	transientEntries map[int]*EntrySpec
	entriesByKey     map[[sha256.Size]byte]int

	preconditions []Precondition

	mvccReadSet *mvccReadSet // mvcc read-set

	metadata *TxMetadata

	ts time.Time

	closed bool
}

type mvccReadSet struct {
	expectedGets           []expectedGet
	expectedGetsWithPrefix []expectedGetWithPrefix
	expectedReaders        []*expectedReader
	readsetSize            int
}

func (mvccReadSet *mvccReadSet) isEmpty() bool {
	return len(mvccReadSet.expectedGets) == 0 &&
		len(mvccReadSet.expectedGetsWithPrefix) == 0 &&
		len(mvccReadSet.expectedReaders) == 0
}

type expectedGet struct {
	key        []byte
	filters    []FilterFn
	expectedTx uint64 // 0 used to denote non-existence
}

type expectedGetWithPrefix struct {
	prefix      []byte
	neq         []byte
	filters     []FilterFn
	expectedKey []byte
	expectedTx  uint64 // 0 used to denote non-existence
}

type EntrySpec struct {
	Key      []byte
	Metadata *KVMetadata
	Value    []byte
	// hashValue is the hash of the value
	// if the actual value is truncated. This is
	// used during replication.
	HashValue [sha256.Size]byte
	// isValueTruncated is true if the value is
	// truncated. This is used during replication.
	IsValueTruncated bool
}

func newOngoingTx(ctx context.Context, s *ImmuStore, opts *TxOptions) (*OngoingTx, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	err := opts.Validate()
	if err != nil {
		return nil, err
	}

	tx := &OngoingTx{
		st:               s,
		ctx:              ctx,
		transientEntries: make(map[int]*EntrySpec),
		entriesByKey:     make(map[[sha256.Size]byte]int),
		ts:               time.Now(),
		unsafeMVCC:       opts.UnsafeMVCC,
	}

	tx.mode = opts.Mode

	if opts.Mode != WriteOnlyTx {
		return tx, nil
	}

	tx.snapshotMustIncludeTxID = opts.SnapshotMustIncludeTxID
	tx.snapshotRenewalPeriod = opts.SnapshotRenewalPeriod

	if opts.Mode == ReadWriteTx {
		tx.mvccReadSet = &mvccReadSet{}
	}
	return tx, nil
}

type ongoingValRef struct {
	value []byte
	hc    uint64
	txmd  *TxMetadata
	kvmd  *KVMetadata
}

func (oref *ongoingValRef) Resolve() (val []byte, err error) {
	return oref.value, nil
}

func (oref *ongoingValRef) Tx() uint64 {
	return 0
}

func (oref *ongoingValRef) HC() uint64 {
	return oref.hc
}

func (oref *ongoingValRef) TxMetadata() *TxMetadata {
	return oref.txmd
}

func (oref *ongoingValRef) KVMetadata() *KVMetadata {
	return oref.kvmd
}

func (oref *ongoingValRef) HVal() [sha256.Size]byte {
	return sha256.Sum256(oref.value)
}

func (oref *ongoingValRef) Len() uint32 {
	return uint32(len(oref.value))
}

func (oref *ongoingValRef) VOff() int64 {
	return 0
}

func (tx *OngoingTx) IsWriteOnly() bool {
	return tx.mode == WriteOnlyTx
}

func (tx *OngoingTx) IsReadOnly() bool {
	return tx.mode == ReadOnlyTx
}

func (tx *OngoingTx) WithMetadata(md *TxMetadata) *OngoingTx {
	tx.metadata = md
	return tx
}

func (tx *OngoingTx) Timestamp() time.Time {
	return tx.ts.Truncate(time.Microsecond).UTC()
}

func (tx *OngoingTx) Metadata() *TxMetadata {
	return tx.metadata
}

func (tx *OngoingTx) snap(key []byte) (*Snapshot, error) {
	for _, snap := range tx.snapshots {
		if hasPrefix(key, snap.prefix) {
			return snap, nil
		}
	}

	var snapshotMustIncludeTxID uint64

	if tx.snapshotMustIncludeTxID != nil {
		snapshotMustIncludeTxID = tx.snapshotMustIncludeTxID(tx.st.LastPrecommittedTxID())
	}

	mandatoryMVCCUpToTxID := tx.st.MandatoryMVCCUpToTxID()

	if mandatoryMVCCUpToTxID > snapshotMustIncludeTxID {
		snapshotMustIncludeTxID = mandatoryMVCCUpToTxID
	}

	snap, err := tx.st.SnapshotMustIncludeTxIDWithRenewalPeriod(tx.ctx, key, snapshotMustIncludeTxID, tx.snapshotRenewalPeriod)
	if err != nil {
		return nil, err
	}

	// using an "interceptor" to construct the valueRef from current entries
	// so to avoid storing more data into the snapshot
	snap.refInterceptor = func(key []byte, valRef ValueRef) ValueRef {
		keyRef, ok := tx.entriesByKey[sha256.Sum256(key)]
		if !ok {
			return valRef
		}

		entrySpec, transient := tx.transientEntries[keyRef]
		if !transient {
			entrySpec = tx.entries[keyRef]
		}

		return &ongoingValRef{
			hc:    valRef.HC(),
			value: entrySpec.Value,
			txmd:  tx.metadata,
			kvmd:  entrySpec.Metadata,
		}
	}

	tx.snapshots = append(tx.snapshots, snap)

	return snap, nil
}

func (tx *OngoingTx) set(key []byte, md *KVMetadata, value []byte, hashValue [sha256.Size]byte, isValueTruncated, isTransient bool) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if tx.IsReadOnly() {
		return ErrReadOnlyTx
	}

	if len(key) == 0 {
		return ErrNullKey
	}

	if len(key) > tx.st.maxKeyLen {
		return ErrMaxKeyLenExceeded
	}

	if len(value) > tx.st.maxValueLen {
		return ErrMaxValueLenExceeded
	}

	kid := sha256.Sum256(key)
	keyRef, isKeyUpdate := tx.entriesByKey[kid]
	if !isKeyUpdate && len(tx.entries) > tx.st.maxTxEntries {
		return ErrMaxTxEntriesLimitExceeded
	}

	_, wasTransient := tx.transientEntries[keyRef]
	if isKeyUpdate {
		if wasTransient != isTransient {
			return ErrCannotUpdateKeyTransiency
		}
	}

	e := &EntrySpec{
		Key:              key,
		Metadata:         md,
		Value:            value,
		HashValue:        hashValue,
		IsValueTruncated: isValueTruncated,
	}

	// vLen=0 + vOff=0 + vHash=0 + txmdLen=0 + kvmdLen=0
	var indexedValue [lszSize + offsetSize + sha256.Size + sszSize + sszSize]byte

	tx.st.indexersMux.RLock()
	indexers := tx.st.indexers
	tx.st.indexersMux.RUnlock()

	for _, indexer := range indexers {
		if isTransient && !hasPrefix(key, indexer.TargetPrefix()) {
			continue
		}

		if !isTransient && (!hasPrefix(key, indexer.SourcePrefix()) || indexer.spec.SourceEntryMapper != nil) {
			continue
		}

		var targetKey []byte

		if isTransient {
			targetKey = key
		} else {
			// map the key, get the snapshot for mapped key, set
			sourceKey, err := mapKey(key, value, indexer.spec.SourceEntryMapper)
			if err != nil {
				return err
			}

			targetKey, err = mapKey(sourceKey, value, indexer.spec.TargetEntryMapper)
			if err != nil {
				return err
			}
		}

		isIndexable := md == nil || !md.NonIndexable()

		// updates are not needed because valueRef are resolved with the "interceptor"
		if !tx.IsWriteOnly() && !isKeyUpdate && isIndexable {
			snap, err := tx.snap(targetKey)
			if err != nil {
				return err
			}

			err = snap.set(targetKey, indexedValue[:])
			if err != nil {
				return err
			}
		}

		if !bytes.Equal(key, targetKey) {
			kid := sha256.Sum256(targetKey)
			keyRef, isKeyUpdate := tx.entriesByKey[kid]

			if isKeyUpdate {
				tx.transientEntries[keyRef] = e
			} else {
				tx.transientEntries[len(tx.entriesByKey)] = e
				tx.entriesByKey[kid] = len(tx.entriesByKey)
			}
		}
	}

	if isKeyUpdate {
		if isTransient {
			tx.transientEntries[keyRef] = e
		} else {
			tx.entries[keyRef] = e
		}
	} else {
		if isTransient {
			tx.transientEntries[len(tx.entriesByKey)] = e
			tx.entriesByKey[kid] = len(tx.entriesByKey)
		} else {
			tx.entries = append(tx.entries, e)
			tx.entriesByKey[kid] = len(tx.entries) - 1
		}

	}

	return nil
}

func mapKey(key []byte, value []byte, mapper EntryMapper) (mappedKey []byte, err error) {
	if mapper == nil {
		return key, nil
	}
	return mapper(key, value)
}

func (tx *OngoingTx) Set(key []byte, md *KVMetadata, value []byte) error {
	var hashValue [sha256.Size]byte
	return tx.set(key, md, value, hashValue, false, false)
}

func (tx *OngoingTx) SetTransient(key []byte, md *KVMetadata, value []byte) error {
	var hashValue [sha256.Size]byte
	return tx.set(key, md, value, hashValue, false, true)
}

func (tx *OngoingTx) AddPrecondition(c Precondition) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if c == nil {
		return ErrIllegalArguments
	}

	err := c.Validate(tx.st)
	if err != nil {
		return err
	}

	tx.preconditions = append(tx.preconditions, c)
	return nil
}

func (tx *OngoingTx) mvccReadSetLimitReached() bool {
	return tx.mvccReadSet.readsetSize == tx.st.mvccReadSetLimit
}

func (tx *OngoingTx) Delete(ctx context.Context, key []byte) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	if tx.IsReadOnly() {
		return ErrReadOnlyTx
	}

	valRef, err := tx.Get(ctx, key)
	if err != nil {
		return err
	}

	if valRef.KVMetadata() != nil && valRef.KVMetadata().Deleted() {
		return ErrKeyNotFound
	}

	md := NewKVMetadata()

	md.AsDeleted(true)

	return tx.Set(key, md, nil)
}

func (tx *OngoingTx) Get(ctx context.Context, key []byte) (ValueRef, error) {
	return tx.GetWithFilters(ctx, key, IgnoreExpired, IgnoreDeleted)
}

func (tx *OngoingTx) GetWithFilters(ctx context.Context, key []byte, filters ...FilterFn) (ValueRef, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, ErrWriteOnlyTx
	}

	snap, err := tx.snap(key)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}

	valRef, err := snap.GetWithFilters(ctx, key, filters...)
	if !tx.IsReadOnly() && errors.Is(err, ErrKeyNotFound) {
		expectedGet := expectedGet{
			key:     cp(key),
			filters: filters,
		}

		if tx.mvccReadSetLimitReached() {
			return nil, ErrMVCCReadSetLimitExceeded
		}

		tx.mvccReadSet.expectedGets = append(tx.mvccReadSet.expectedGets, expectedGet)
		tx.mvccReadSet.readsetSize++
	}
	if err != nil {
		return nil, err
	}

	if !tx.IsReadOnly() && valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx
		expectedGet := expectedGet{
			key:        cp(key),
			filters:    filters,
			expectedTx: valRef.Tx(),
		}

		if tx.mvccReadSetLimitReached() {
			return nil, ErrMVCCReadSetLimitExceeded
		}

		tx.mvccReadSet.expectedGets = append(tx.mvccReadSet.expectedGets, expectedGet)
		tx.mvccReadSet.readsetSize++
	}

	return valRef, nil
}

func (tx *OngoingTx) GetWithPrefix(ctx context.Context, prefix, neq []byte) (key []byte, valRef ValueRef, err error) {
	return tx.GetWithPrefixAndFilters(ctx, prefix, neq, IgnoreExpired, IgnoreDeleted)
}

func (tx *OngoingTx) GetWithPrefixAndFilters(ctx context.Context, prefix, neq []byte, filters ...FilterFn) (key []byte, valRef ValueRef, err error) {
	if tx.closed {
		return nil, nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, nil, ErrWriteOnlyTx
	}

	snap, err := tx.snap(prefix)
	if err != nil {
		if errors.Is(err, ErrIndexNotFound) {
			return nil, nil, ErrKeyNotFound
		}
		return nil, nil, err
	}

	key, valRef, err = snap.GetWithPrefixAndFilters(ctx, prefix, neq, filters...)
	if !tx.IsReadOnly() && errors.Is(err, ErrKeyNotFound) {
		expectedGetWithPrefix := expectedGetWithPrefix{
			prefix:  cp(prefix),
			neq:     cp(neq),
			filters: filters,
		}

		if tx.mvccReadSetLimitReached() {
			return nil, nil, ErrMVCCReadSetLimitExceeded
		}

		tx.mvccReadSet.expectedGetsWithPrefix = append(tx.mvccReadSet.expectedGetsWithPrefix, expectedGetWithPrefix)
		tx.mvccReadSet.readsetSize++
	}
	if err != nil {
		return nil, nil, err
	}

	if !tx.IsReadOnly() && valRef.Tx() > 0 {
		// it only requires validation when the entry was pre-existent to ongoing tx
		expectedGetWithPrefix := expectedGetWithPrefix{
			prefix:      cp(prefix),
			neq:         cp(neq),
			filters:     filters,
			expectedKey: cp(key),
			expectedTx:  valRef.Tx(),
		}

		if tx.mvccReadSetLimitReached() {
			return nil, nil, ErrMVCCReadSetLimitExceeded
		}

		tx.mvccReadSet.expectedGetsWithPrefix = append(tx.mvccReadSet.expectedGetsWithPrefix, expectedGetWithPrefix)
		tx.mvccReadSet.readsetSize++
	}

	return key, valRef, nil
}

func (tx *OngoingTx) NewKeyReader(spec KeyReaderSpec) (KeyReader, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if tx.IsWriteOnly() {
		return nil, ErrWriteOnlyTx
	}

	if tx.IsReadOnly() {
		snap, err := tx.snap(spec.Prefix)
		if err != nil {
			return nil, err
		}

		return snap.NewKeyReader(spec)
	}
	return newOngoingTxKeyReader(tx, spec)
}

func (tx *OngoingTx) RequireMVCCOnFollowingTxs(requireMVCCOnFollowingTxs bool) error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	tx.requireMVCCOnFollowingTxs = requireMVCCOnFollowingTxs

	return nil
}

func (tx *OngoingTx) Commit(ctx context.Context) (*TxHeader, error) {
	return tx.commit(ctx, true)
}

func (tx *OngoingTx) AsyncCommit(ctx context.Context) (*TxHeader, error) {
	return tx.commit(ctx, false)
}

func (tx *OngoingTx) commit(ctx context.Context, waitForIndexing bool) (*TxHeader, error) {
	if tx.closed {
		return nil, ErrAlreadyClosed
	}

	if !tx.IsWriteOnly() {
		for _, snap := range tx.snapshots {
			err := snap.Close()
			if err != nil {
				return nil, err
			}
		}
	}

	tx.closed = true

	if tx.IsReadOnly() {
		return nil, ErrReadOnlyTx
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return tx.st.commit(ctx, tx, nil, false, waitForIndexing)
}

func (tx *OngoingTx) Cancel() error {
	if tx.closed {
		return ErrAlreadyClosed
	}

	tx.closed = true

	if !tx.IsWriteOnly() {
		for _, snap := range tx.snapshots {
			err := snap.Close()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tx *OngoingTx) Closed() bool {
	return tx.closed
}

func (tx *OngoingTx) hasPreconditions() bool {
	return len(tx.preconditions) > 0 || (tx.mvccReadSet != nil && !tx.mvccReadSet.isEmpty())
}

func (tx *OngoingTx) checkPreconditions(ctx context.Context, st *ImmuStore) error {
	for _, c := range tx.preconditions {
		if c == nil {
			return ErrInvalidPreconditionNull
		}
		ok, err := c.Check(ctx, st)
		if err != nil {
			return fmt.Errorf("error checking %s precondition: %w", c, err)
		}
		if !ok {
			return fmt.Errorf("%w: %s", ErrPreconditionFailed, c)
		}
	}

	if tx.IsWriteOnly() {
		// read-only transactions won't be invalidated
		return nil
	}

	for _, txSnap := range tx.snapshots {
		if txSnap.Ts() > st.LastPrecommittedTxID() {
			// read-write transactions when no other transaction was committed won't be invalidated
			return nil
		}

		// current snapshot is fetched without flushing
		snap, err := st.syncSnapshot(txSnap.prefix)
		if err != nil {
			return err
		}
		defer snap.Close()

		for _, e := range tx.mvccReadSet.expectedGets {
			if !hasPrefix(e.key, txSnap.prefix) {
				continue
			}

			valRef, err := snap.GetWithFilters(ctx, e.key, e.filters...)
			if errors.Is(err, ErrKeyNotFound) {
				if e.expectedTx > 0 {
					return ErrTxReadConflict
				}
				continue
			}
			if err != nil {
				return err
			}

			if e.expectedTx != valRef.Tx() {
				return ErrTxReadConflict
			}
		}

		for _, e := range tx.mvccReadSet.expectedGetsWithPrefix {
			if !hasPrefix(e.prefix, txSnap.prefix) {
				continue
			}

			key, valRef, err := snap.GetWithPrefixAndFilters(ctx, e.prefix, e.neq, e.filters...)
			if errors.Is(err, ErrKeyNotFound) {
				if e.expectedTx > 0 {
					return ErrTxReadConflict
				}
				continue
			}
			if err != nil {
				return err
			}

			if !bytes.Equal(e.expectedKey, key) || e.expectedTx != valRef.Tx() {
				return ErrTxReadConflict
			}
		}

		for _, eReader := range tx.mvccReadSet.expectedReaders {
			if !hasPrefix(eReader.spec.Prefix, txSnap.prefix) {
				continue
			}

			rspec := KeyReaderSpec{
				SeekKey:       eReader.spec.SeekKey,
				EndKey:        eReader.spec.EndKey,
				Prefix:        eReader.spec.Prefix,
				InclusiveSeek: eReader.spec.InclusiveSeek,
				InclusiveEnd:  eReader.spec.InclusiveEnd,
				DescOrder:     eReader.spec.DescOrder,
			}

			reader, err := snap.NewKeyReader(rspec)
			if err != nil {
				return err
			}

			defer reader.Close()

			for _, eReads := range eReader.expectedReads {
				var key []byte
				var valRef ValueRef

				for _, eRead := range eReads {

					if len(key) == 0 {
						if eRead.initialTxID == 0 && eRead.finalTxID == 0 {
							key, valRef, err = reader.Read(ctx)
						} else {
							key, valRef, err = reader.ReadBetween(ctx, eRead.initialTxID, eRead.finalTxID)
						}

						if err != nil && !errors.Is(err, ErrNoMoreEntries) {
							return err
						}
					}

					if eRead.expectedNoMoreEntries {
						if err == nil {
							return fmt.Errorf("%w: fetching more entries than expected", ErrTxReadConflict)
						}

						break
					}

					if eRead.expectedTx == 0 {
						if err == nil && bytes.Equal(eRead.expectedKey, key) {
							// key was updated by the transaction
							key = nil
							valRef = nil
						}
					} else {
						if errors.Is(err, ErrNoMoreEntries) {
							return fmt.Errorf("%w: fetching less entries than expected", ErrTxReadConflict)
						}

						if !bytes.Equal(eRead.expectedKey, key) || eRead.expectedTx != valRef.Tx() {
							return fmt.Errorf("%w: fetching a different key or an updated one", ErrTxReadConflict)
						}

						key = nil
						valRef = nil
					}
				}

				err = reader.Reset()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (tx *OngoingTx) validateAgainst(hdr *TxHeader) error {
	if hdr == nil {
		return nil
	}

	if len(tx.entries) != hdr.NEntries {
		return fmt.Errorf("%w: number of entries differs", ErrIllegalArguments)
	}

	if tx.metadata != nil {
		if !tx.metadata.Equal(hdr.Metadata) {
			return fmt.Errorf("%w: metadata differs", ErrIllegalArguments)
		}
	} else if hdr.Metadata != nil {
		if !hdr.Metadata.Equal(tx.metadata) {
			return fmt.Errorf("%w: metadata differs", ErrIllegalArguments)
		}
	}

	return nil
}

func (tx *OngoingTx) Context() context.Context {
	return tx.ctx
}

func cp(s []byte) []byte {
	if s == nil {
		return nil
	}

	c := make([]byte, len(s))
	copy(c, s)

	return c
}
