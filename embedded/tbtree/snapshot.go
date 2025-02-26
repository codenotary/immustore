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
	"encoding/binary"
	"io"
	"math"
	"sync"
)

const (
	InnerNodeType = iota
	LeafNodeType
)

// Snapshot implements a snapshot on top of a B-tree data structure.
// It provides methods for storing and retrieving key-value pairs.
// The snapshot maintains a consistent view of the underlying data structure.
// It uses a lock to ensure concurrent access safety.
// Snapshot represents a consistent view of a B-tree data structure.
type Snapshot struct {
	t           *TBtree
	id          uint64
	ts          uint64
	root        node
	readers     map[int]io.Closer
	maxReaderID int
	closed      bool

	_buf []byte

	mutex sync.RWMutex
}

// Set inserts a key-value pair into the snapshot.
// It locks the snapshot, performs the insertion, and updates the root node if necessary.
// The method handles splitting of nodes to maintain the B-tree structure.
// It returns an error if the insertion fails.
// Example usage:
//
//	err := snapshot.Set([]byte("key"), []byte("value"))
func (s *Snapshot) Set(key, value []byte) error {
	// Acquire a write lock on the snapshot
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Create copies of the key and value to ensure immutability
	k := make([]byte, len(key))
	copy(k, key)

	v := make([]byte, len(value))
	copy(v, value)

	// Insert the key-value pair into the root node
	nodes, depth, err := s.root.insert([]*KVT{{K: k, V: v, T: s.ts}})
	if err != nil {
		return err
	}

	// Split nodes to maintain the B-tree structure
	for len(nodes) > 1 {
		newRoot := &innerNode{
			t:     s.t,
			nodes: nodes,
			_ts:   s.ts,
			mut:   true,
		}

		depth++

		nodes, err = newRoot.split()
		if err != nil {
			return err
		}
	}

	// Update the root node
	s.root = nodes[0]

	// Update B-tree depth metric
	metricsBtreeDepth.WithLabelValues(s.t.path).Set(float64(depth))

	return nil
}

// Get retrieves the value associated with the given key from the snapshot.
// It locks the snapshot for reading, and delegates the retrieval to the root node.
// The method returns the value, timestamp, hash count, and an error.
// Example usage:
//
//	value, timestamp, hashCount, err := snapshot.Get([]byte("key"))
func (s *Snapshot) Get(key []byte) (value []byte, ts uint64, hc uint64, err error) {
	// Acquire a read lock on the snapshot
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Check if the snapshot is closed
	if s.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	// Check if the key argument is nil
	if key == nil {
		return nil, 0, 0, ErrIllegalArguments
	}

	// Delegate the retrieval to the root node
	v, ts, hc, err := s.root.get(key)
	return cp(v), ts, hc, err
}

func (s *Snapshot) GetBetween(key []byte, initialTs, finalTs uint64) (value []byte, ts uint64, hc uint64, err error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.closed {
		return nil, 0, 0, ErrAlreadyClosed
	}

	if key == nil {
		return nil, 0, 0, ErrIllegalArguments
	}

	v, ts, hc, err := s.root.getBetween(key, initialTs, finalTs)
	return cp(v), ts, hc, err
}

// History retrieves the history of a key in the snapshot.
// It locks the snapshot for reading, and delegates the history retrieval to the root node.
// The method returns an array of timestamps, the hash count, and an error.
// Example usage:
//
//	timestamps, hashCount, err := snapshot.History([]byte("key"), 0, true, 10)
func (s *Snapshot) History(key []byte, offset uint64, descOrder bool, limit int) (timedValues []TimedValue, hCount uint64, err error) {
	// Acquire a read lock on the snapshot
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Check if the snapshot is closed
	if s.closed {
		return nil, 0, ErrAlreadyClosed
	}

	// Check if the key argument is nil
	if key == nil {
		return nil, 0, ErrIllegalArguments
	}

	// Check if the limit argument is less than 1
	if limit < 1 {
		return nil, 0, ErrIllegalArguments
	}

	// Delegate the history retrieval to the root node
	return s.root.history(key, offset, descOrder, limit)
}

// Ts returns the timestamp associated with the root node of the snapshot.
// It locks the snapshot for reading and returns the timestamp.
// Example usage:
//
//	timestamp := snapshot.Ts()
func (s *Snapshot) Ts() uint64 {
	// Acquire a read lock on the snapshot
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.root.ts()
}

// GetWithPrefix retrieves the key-value pair with a specific prefix from the snapshot.
// It locks the snapshot for reading, and delegates the retrieval to the root node.
// The method returns the key, value, timestamp, hash count, and an error.
// Example usage:
//
//	key, value, timestamp, hashCount, err := snapshot.GetWithPrefix([]byte("prefix"), []byte("neq"))
func (s *Snapshot) GetWithPrefix(prefix []byte, neq []byte) (key []byte, value []byte, ts uint64, hc uint64, err error) {
	// Acquire a read lock on the snapshot
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Check if the snapshot is closed
	if s.closed {
		return nil, nil, 0, 0, ErrAlreadyClosed
	}

	// Find the leaf node containing the key-value pair
	_, leaf, off, err := s.root.findLeafNode(prefix, nil, 0, neq, false)
	if err != nil {
		return nil, nil, 0, 0, err
	}

	// Retrieve the leaf value at the specified offset
	leafValue := leaf.values[off]

	// Check if the prefix matches the leaf key
	if len(prefix) > len(leafValue.key) {
		return nil, nil, 0, 0, ErrKeyNotFound
	}

	if bytes.Equal(prefix, leafValue.key[:len(prefix)]) {
		return leafValue.key, cp(leafValue.timedValue().Value), leafValue.timedValue().Ts, leafValue.historyCount(), nil
	}

	return nil, nil, 0, 0, ErrKeyNotFound
}

// NewHistoryReader creates a new history reader for the snapshot.
// It locks the snapshot for reading and creates a new history reader based on the given specification.
// The method returns the history reader and an error if the creation fails.
// Example usage:
//
//	reader, err := snapshot.NewHistoryReader(&HistoryReaderSpec{Key: []byte("key"), Limit: 10})
func (s *Snapshot) NewHistoryReader(spec *HistoryReaderSpec) (*HistoryReader, error) {
	// Acquire a read lock on the snapshot
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Check if the snapshot is closed
	if s.closed {
		return nil, ErrAlreadyClosed
	}

	// Create a new history reader with the given specification
	reader, err := newHistoryReader(s.maxReaderID, s, spec)
	if err != nil {
		return nil, err
	}

	// Store the reader in the snapshot's readers map
	s.readers[reader.id] = reader
	s.maxReaderID++

	return reader, nil
}

// NewReader creates a new reader for the snapshot.
// It locks the snapshot for writing and creates a new reader based on the given specification.
// The method returns the reader and an error if the creation fails.
// Example usage:
//
//	reader, err := snapshot.NewReader(ReaderSpec{Prefix: []byte("prefix"), DescOrder: true})
func (s *Snapshot) NewReader(spec ReaderSpec) (r *Reader, err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return nil, ErrAlreadyClosed
	}

	if len(spec.SeekKey) > s.t.maxKeySize || len(spec.Prefix) > s.t.maxKeySize {
		return nil, ErrIllegalArguments
	}

	greatestPrefixedKey := greatestKeyOfSize(s.t.maxKeySize)
	copy(greatestPrefixedKey, spec.Prefix)

	// Adjust seekKey based on key prefix
	seekKey := spec.SeekKey
	inclusiveSeek := spec.InclusiveSeek

	if spec.DescOrder {
		if len(spec.SeekKey) == 0 || bytes.Compare(spec.SeekKey, greatestPrefixedKey) > 0 {
			seekKey = greatestPrefixedKey
			inclusiveSeek = true
		}
	} else {
		if bytes.Compare(spec.SeekKey, spec.Prefix) < 0 {
			seekKey = spec.Prefix
			inclusiveSeek = true
		}
	}

	// Adjust endKey based on key prefix
	endKey := spec.EndKey
	inclusiveEnd := spec.InclusiveEnd

	if spec.DescOrder {
		if bytes.Compare(spec.EndKey, spec.Prefix) < 0 {
			endKey = spec.Prefix
			inclusiveEnd = true
		}
	} else {
		if len(spec.EndKey) == 0 || bytes.Compare(spec.EndKey, greatestPrefixedKey) > 0 {
			endKey = greatestPrefixedKey
			inclusiveEnd = true
		}
	}

	// Create a new reader with the given specification
	r = &Reader{
		snapshot:       s,
		id:             s.maxReaderID,
		seekKey:        seekKey,
		endKey:         endKey,
		prefix:         spec.Prefix,
		inclusiveSeek:  inclusiveSeek,
		inclusiveEnd:   inclusiveEnd,
		includeHistory: spec.IncludeHistory,
		descOrder:      spec.DescOrder,
		offset:         spec.Offset,
		closed:         false,
	}

	s.readers[r.id] = r
	s.maxReaderID++

	return r, nil
}

// closedReader removes a closed reader from the snapshot's readers map.
// It locks the snapshot for writing and removes the reader with the specified ID.
// The method returns an error if the removal fails.
func (s *Snapshot) closedReader(id int) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.readers, id)
	return nil
}

// Close closes the snapshot and releases any associated resources.
// It locks the snapshot for writing, checks if there are any active readers, and marks the snapshot as closed.
// The method returns an error if there are active readers.
// Example usage:
//
//	err := snapshot.Close()
func (s *Snapshot) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return ErrAlreadyClosed
	}

	if len(s.readers) > 0 {
		return ErrReadersNotClosed
	}

	err := s.t.snapshotClosed(s)
	if err != nil {
		return err
	}

	s.closed = true

	return nil
}

// WriteTo writes the snapshot to the specified writers.
// It locks the snapshot for writing, performs the write operation on the root node,
// and returns the root offset, minimum offset, number of bytes written to nw and hw,
// and an error if any.
//
// Parameters:
// - nw: The writer to write the snapshot's nodes.
// - hw: The writer to write the snapshot's history.
// - writeOpts: The options for the write operation.
//
// Returns:
// - rootOffset: The offset of the root node in the written data.
// - minOffset: The minimum offset of all written nodes.
// - wN: The number of bytes written to nw.
// - wH: The number of bytes written to hw.
// - err: An error if the write operation fails or the arguments are invalid.
//
// Example usage:
//
//	rootOffset, minOffset, wN, wH, err := snapshot.WriteTo(nw, hw, &WriteOpts{})
func (s *Snapshot) WriteTo(nw, hw io.Writer, writeOpts *WriteOpts) (rootOffset, minOffset int64, wN, wH int64, err error) {
	if nw == nil || writeOpts == nil {
		return 0, 0, 0, 0, ErrIllegalArguments
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.root.writeTo(nw, hw, writeOpts, s._buf)
}

func (n *innerNode) writeTo(nw, hw io.Writer, writeOpts *WriteOpts, buf []byte) (nOff, minOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && !n.mutated() && n._minOff >= writeOpts.MinOffset {
		return n.off, n._minOff, 0, 0, nil
	}

	var cnw, chw int64

	wopts := &WriteOpts{
		OnlyMutated:    writeOpts.OnlyMutated,
		commitLog:      writeOpts.commitLog,
		reportProgress: writeOpts.reportProgress,
		MinOffset:      writeOpts.MinOffset,
	}

	offsets := make([]int64, len(n.nodes))
	minOffsets := make([]int64, len(n.nodes))
	minOff = math.MaxInt64

	for i, c := range n.nodes {
		wopts.BaseNLogOffset = writeOpts.BaseNLogOffset + cnw
		wopts.BaseHLogOffset = writeOpts.BaseHLogOffset + chw

		no, mo, wn, wh, err := c.writeTo(nw, hw, wopts, buf)
		if err != nil {
			return 0, 0, cnw, chw, err
		}

		offsets[i] = no
		minOffsets[i] = mo

		if minOffsets[i] < minOff {
			minOff = minOffsets[i]
		}

		cnw += wn
		chw += wh
	}

	size, err := n.size()
	if err != nil {
		return 0, 0, cnw, chw, err
	}

	bi := 0

	buf[bi] = InnerNodeType
	bi++

	if n.tsMutated() {
		binary.BigEndian.PutUint16(buf[bi:], uint16(len(n.nodes)+1))
		bi += 2

		bi += writeNodeRefToWithOffset(n, math.MaxInt64, math.MaxInt64, buf[bi:])
	} else {
		binary.BigEndian.PutUint16(buf[bi:], uint16(len(n.nodes)))
		bi += 2
	}

	for i, c := range n.nodes {
		n := writeNodeRefToWithOffset(c, offsets[i], minOffsets[i], buf[bi:])
		bi += n
	}

	wn, err := nw.Write(buf[:bi])
	if err != nil {
		return 0, 0, int64(wn), chw, err
	}

	wN = cnw + int64(size)
	nOff = writeOpts.BaseNLogOffset + cnw

	if writeOpts.commitLog {
		n.off = writeOpts.BaseNLogOffset + cnw
		n._minOff = minOff

		if n.mut {
			n.mut = false

			for i, c := range n.nodes {
				_, isNodeRef := c.(*nodeRef)

				if isNodeRef {
					continue
				}

				n.nodes[i] = &nodeRef{
					t:       n.t,
					_minKey: c.minKey(),
					_ts:     c.ts(),
					off:     c.offset(),
					_minOff: c.minOffset(),
				}

				n.t.cachePut(c)
			}
		}
	}

	writeOpts.reportProgress(1, 0, 0)

	return nOff, minOff, wN, chw, nil
}

func (l *leafNode) writeTo(nw, hw io.Writer, writeOpts *WriteOpts, buf []byte) (nOff, minOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && !l.mutated() && l.off >= writeOpts.MinOffset {
		return l.off, l.off, 0, 0, nil
	}

	size, err := l.size()
	if err != nil {
		return 0, 0, 0, 0, err
	}

	bi := 0

	buf[bi] = LeafNodeType
	bi++

	if l.tsMutated() {
		// NOTE: we store a marker entry to remember the highest timestamp seen

		binary.BigEndian.PutUint16(buf[bi:], uint16(len(l.values)+1))
		bi += 2

		binary.BigEndian.PutUint16(buf[bi:], 0)
		bi += 2

		binary.BigEndian.PutUint64(buf[bi:], l._ts)
		bi += 8
	} else {
		binary.BigEndian.PutUint16(buf[bi:], uint16(len(l.values)))
		bi += 2
	}

	accH := int64(0)
	for _, v := range l.values {
		timedValue := v.timedValues[0]

		binary.BigEndian.PutUint16(buf[bi:], uint16(len(v.key)))
		bi += 2

		copy(buf[bi:], v.key)
		bi += len(v.key)

		binary.BigEndian.PutUint16(buf[bi:], uint16(len(timedValue.Value)))
		bi += 2

		copy(buf[bi:], timedValue.Value)
		bi += len(timedValue.Value)

		binary.BigEndian.PutUint64(buf[bi:], timedValue.Ts)
		bi += 8

		hOff := v.hOff

		if len(v.timedValues) > 1 {
			hbuf := new(bytes.Buffer)

			binary.Write(hbuf, binary.BigEndian, uint32(len(v.timedValues)-1))

			for _, tv := range v.timedValues[1:] {

				binary.Write(hbuf, binary.BigEndian, uint16(len(tv.Value)))

				hbuf.Write(tv.Value)

				binary.Write(hbuf, binary.BigEndian, uint64(tv.Ts))
			}

			binary.Write(hbuf, binary.BigEndian, uint64(v.hOff))

			hOff = writeOpts.BaseHLogOffset + accH

			n, err := hw.Write(hbuf.Bytes())
			if err != nil {
				return 0, 0, 0, int64(n), err
			}

			accH += int64(n)
		}

		binary.BigEndian.PutUint64(buf[bi:], uint64(hOff))
		bi += 8

		hCount := v.historyCount()

		binary.BigEndian.PutUint64(buf[bi:], hCount-1)
		bi += 8

		if writeOpts.commitLog {
			v.timedValues = v.timedValues[:1]
			v.hOff = hOff
			v.hCount = hCount - 1
		}
	}

	n, err := nw.Write(buf[:bi])
	if err != nil {
		return 0, 0, int64(n), accH, err
	}

	wN = int64(size)
	nOff = writeOpts.BaseNLogOffset

	if writeOpts.commitLog {
		l.off = nOff

		if l.mut {
			l.mut = false
			l.t.cachePut(l)
		}
	}

	writeOpts.reportProgress(0, 1, len(l.values))

	return nOff, nOff, wN, accH, nil
}

func (n *nodeRef) writeTo(nw, hw io.Writer, writeOpts *WriteOpts, buf []byte) (nOff, minOff int64, wN, wH int64, err error) {
	if writeOpts.OnlyMutated && n._minOff >= writeOpts.MinOffset {
		return n.off, n._minOff, 0, 0, nil
	}

	node, err := n.t.nodeAt(n.off, false)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	off, mOff, wn, wh, err := node.writeTo(nw, hw, writeOpts, buf)
	if err != nil {
		return 0, 0, wn, wh, err
	}

	if writeOpts.commitLog {
		n.off = off
		n._minOff = mOff
	}

	return off, mOff, wn, wh, nil
}

func writeNodeRefToWithOffset(n node, offset, minOff int64, buf []byte) int {
	i := 0

	minKey := n.minKey()
	binary.BigEndian.PutUint16(buf[i:], uint16(len(minKey)))
	i += 2

	copy(buf[i:], minKey)
	i += len(minKey)

	binary.BigEndian.PutUint64(buf[i:], n.ts())
	i += 8

	binary.BigEndian.PutUint64(buf[i:], uint64(offset))
	i += 8

	binary.BigEndian.PutUint64(buf[i:], uint64(minOff))
	i += 8

	return i
}
