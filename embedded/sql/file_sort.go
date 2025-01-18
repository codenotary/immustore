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

package sql

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
)

type sortedChunk struct {
	offset uint64
	size   uint64
}

type fileSorter struct {
	colPosBySelector map[string]int
	colTypes         []string
	cmp              func(r1, r2 *Row) (int, error)

	tx          *SQLTx
	sortBufSize int
	sortBuf     []*Row
	nextIdx     int

	tempFile     *os.File
	writer       *bufio.Writer
	tempFileSize uint64

	chunksToMerge []sortedChunk
}

func (s *fileSorter) update(r *Row) error {
	if s.nextIdx == s.sortBufSize {
		err := s.sortAndFlushBuffer()
		if err != nil {
			return err
		}
		s.nextIdx = 0
	}

	s.sortBuf[s.nextIdx] = r
	s.nextIdx++

	return nil
}

func (s *fileSorter) finalize() (resultReader, error) {
	if s.nextIdx > 0 {
		if err := s.sortBuffer(); err != nil {
			return nil, err
		}
	}

	// result rows are all in memory
	if len(s.chunksToMerge) == 0 {
		return &bufferResultReader{
			sortBuf: s.sortBuf[:s.nextIdx],
		}, nil
	}

	err := s.flushBuffer()
	if err != nil {
		return nil, err
	}

	err = s.writer.Flush()
	if err != nil {
		return nil, err
	}
	return s.mergeAllChunks()
}

func (s *fileSorter) mergeAllChunks() (resultReader, error) {
	currFile := s.tempFile

	outFile, err := s.tx.createTempFile()
	if err != nil {
		return nil, err
	}

	lbuf := &bufio.Reader{}
	rbuf := &bufio.Reader{}

	lr := &fileRowReader{
		colPosBySelector: s.colPosBySelector,
		colTypes:         s.colTypes,
		reader:           lbuf,
		reuseRow:         true,
	}
	rr := &fileRowReader{
		colPosBySelector: s.colPosBySelector,
		colTypes:         s.colTypes,
		reader:           rbuf,
		reuseRow:         true,
	}

	chunks := s.chunksToMerge
	for len(chunks) > 1 {
		s.writer.Reset(outFile)

		var offset uint64

		newChunks := make([]sortedChunk, (len(chunks)+1)/2)
		for i := 0; i < len(chunks)/2; i++ {
			c1 := chunks[i*2]
			c2 := chunks[i*2+1]

			lbuf.Reset(io.NewSectionReader(currFile, int64(c1.offset), int64(c1.size)))
			rbuf.Reset(io.NewSectionReader(currFile, int64(c2.offset), int64(c2.size)))

			err := s.mergeChunks(lr, rr, s.writer)
			if err != nil {
				return nil, err
			}

			newChunks[i] = sortedChunk{
				offset: offset,
				size:   c1.size + c2.size,
			}
			offset += c1.size + c2.size
		}

		err := s.writer.Flush()
		if err != nil {
			return nil, err
		}

		if len(chunks)%2 != 0 { // copy last sorted chunk
			lastChunk := chunks[len(chunks)-1]

			_, err := io.Copy(outFile, io.NewSectionReader(currFile, int64(lastChunk.offset), int64(lastChunk.size)))
			if err != nil {
				return nil, err
			}
			newChunks[len(chunks)/2] = lastChunk
		}

		currFile, outFile = outFile, currFile

		_, err = outFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}

		chunks = newChunks
	}

	return &fileRowReader{
		colTypes:         s.colTypes,
		colPosBySelector: s.colPosBySelector,
		reader:           bufio.NewReader(io.NewSectionReader(currFile, 0, int64(s.tempFileSize))),
	}, nil
}

func (s *fileSorter) mergeChunks(lr, rr *fileRowReader, writer io.Writer) error {
	var err error
	var lrAtEOF bool
	var r1, r2 *Row

	for {
		if r1 == nil {
			r1, err = lr.Read()
			if err == ErrNoMoreRows {
				lrAtEOF = true
				break
			}

			if err != nil {
				return err
			}
		}

		if r2 == nil {
			r2, err = rr.Read()
			if err == ErrNoMoreRows {
				break
			}

			if err != nil {
				return err
			}
		}

		var rawData []byte
		res, err := s.cmp(r1, r2)
		if err != nil {
			return err
		}

		if res < 0 {
			rawData = lr.rowBuf.Bytes()
			r1 = nil
		} else {
			rawData = rr.rowBuf.Bytes()
			r2 = nil
		}

		_, err = writer.Write(rawData)
		if err != nil {
			return err
		}
	}

	readerToCopy := lr
	if lrAtEOF {
		readerToCopy = rr
	}

	_, err = writer.Write(readerToCopy.rowBuf.Bytes())
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, readerToCopy.reader)
	return err
}

type resultReader interface {
	Read() (*Row, error)
}

type bufferResultReader struct {
	sortBuf []*Row
	nextIdx int
}

func (r *bufferResultReader) Read() (*Row, error) {
	if r.nextIdx == len(r.sortBuf) {
		return nil, ErrNoMoreRows
	}

	row := r.sortBuf[r.nextIdx]
	r.nextIdx++
	return row, nil
}

type fileRowReader struct {
	colPosBySelector map[string]int
	colTypes         []SQLValueType
	reader           io.Reader
	rowBuf           bytes.Buffer
	row              *Row
	reuseRow         bool
}

func (r *fileRowReader) readValues(out []TypedValue) error {
	var size uint16
	err := binary.Read(r.reader, binary.BigEndian, &size)
	if err != nil {
		return err
	}

	r.rowBuf.Reset()

	binary.Write(&r.rowBuf, binary.BigEndian, &size)

	_, err = io.CopyN(&r.rowBuf, r.reader, int64(size))
	if err != nil {
		return err
	}

	data := r.rowBuf.Bytes()
	return decodeValues(data[2:], r.colTypes, out)
}

func (r *fileRowReader) Read() (*Row, error) {
	row := r.getRow()

	err := r.readValues(row.ValuesByPosition)
	if err == io.EOF {
		return nil, ErrNoMoreRows
	}
	if err != nil {
		return nil, err
	}

	for sel, pos := range r.colPosBySelector {
		row.ValuesBySelector[sel] = row.ValuesByPosition[pos]
	}
	return row, nil
}

func (r *fileRowReader) getRow() *Row {
	row := r.row
	if row == nil || !r.reuseRow {
		row = &Row{
			ValuesByPosition: make([]TypedValue, len(r.colPosBySelector)),
			ValuesBySelector: make(map[string]TypedValue, len(r.colPosBySelector)),
		}
		r.row = row
	}
	return row
}

func decodeValues(data []byte, colTypes []SQLValueType, out []TypedValue) error {
	var voff int
	for i, col := range colTypes {
		v, n, err := DecodeNullableValue(data[voff:], col)
		if err != nil {
			return err
		}
		voff += n

		out[i] = v
	}
	return nil
}

func (s *fileSorter) sortAndFlushBuffer() error {
	if err := s.sortBuffer(); err != nil {
		return err
	}
	return s.flushBuffer()
}

func (s *fileSorter) sortBuffer() error {
	buf := s.sortBuf[:s.nextIdx]

	var outErr error
	sort.Slice(buf, func(i, j int) bool {
		r1 := buf[i]
		r2 := buf[j]

		res, err := s.cmp(r1, r2)
		if err != nil {
			outErr = err
		}
		return res < 0
	})
	return outErr
}

func (s *fileSorter) flushBuffer() error {
	writer, err := s.tempFileWriter()
	if err != nil {
		return err
	}

	var chunkSize uint64
	for _, row := range s.sortBuf[:s.nextIdx] {
		data, err := encodeRow(row)
		if err != nil {
			return err
		}

		_, err = writer.Write(data)
		if err != nil {
			return err
		}

		chunkSize += uint64(len(data))
	}

	s.chunksToMerge = append(s.chunksToMerge, sortedChunk{
		offset: s.tempFileSize,
		size:   chunkSize,
	})
	s.tempFileSize += chunkSize
	return nil
}

func (s *fileSorter) tempFileWriter() (*bufio.Writer, error) {
	if s.writer != nil {
		return s.writer, nil
	}
	file, err := s.tx.createTempFile()
	if err != nil {
		return nil, err
	}
	s.tempFile = file
	s.writer = bufio.NewWriter(file)
	return s.writer, nil
}

func encodeRow(r *Row) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write([]byte{0, 0}) // make room for size field

	for _, v := range r.ValuesByPosition {
		rawValue, err := EncodeNullableValue(v, v.Type(), -1)
		if err != nil {
			return nil, err
		}
		buf.Write(rawValue)
	}

	data := buf.Bytes()
	size := uint16(len(data) - 2)
	binary.BigEndian.PutUint16(data, size)

	return data, nil
}
