// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package fs

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type entryIter struct {
	idx     int
	entries []entry
}

func (it *entryIter) next() bool {
	it.idx = it.idx + 1
	return it.idx < len(it.entries)
}

func (it *entryIter) current() entry { return it.entries[it.idx] }

func (it *entryIter) assertExhausted(t *testing.T) {
	assert.True(t, it.idx >= len(it.entries))
}

func newEntryReaders(entries ...entry) entryReader {
	return &entryIter{idx: -1, entries: entries}
}

func buildExpectedOutputStream(
	t *testing.T, expected ReadMismatches,
) (chan<- ReadMismatch, *sync.WaitGroup) {
	ch := make(chan ReadMismatch)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		actual := make(ReadMismatches, 0, len(expected))
		for mismatch := range ch {
			actual = append(actual, mismatch)
		}
		assert.Equal(t, expected, actual,
			fmt.Sprintf("mismatch lists do not match\n\nExpected: %+v\nActual:   %+v",
				expected, actual))
		wg.Done()
	}()

	return ch, &wg
}

func buildDataInputStream(bls []ident.IndexHashBlock) <-chan ident.IndexHashBlock {
	ch := make(chan ident.IndexHashBlock)
	go func() {
		for _, bl := range bls {
			ch <- bl
		}
		close(ch)
	}()

	return ch
}

func idxHash(i int) ident.IndexHash {
	return ident.IndexHash{DataChecksum: uint32(i), IDHash: uint64(i)}
}

func idxEntry(i int, id string) entry {
	return entry{
		entry: schema.IndexEntry{
			DataChecksum: int64(i),
			ID:           []byte(id),
		},
		idHash: uint64(i),
	}
}

func mismatch(t MismatchType, i int, id []byte) ReadMismatch {
	m := ReadMismatch{Type: t, Checksum: uint32(i), IDHash: uint64(i)}
	if len(id) > 0 {
		m.ID = ident.BytesID(id)
	}

	return m
}

func assertClosed(t *testing.T, in <-chan ident.IndexHashBlock) {
	_, isOpen := <-in
	require.False(t, isOpen)
}

func TestReportErrorDrainAndClose(t *testing.T) {
	err := errors.New("an error")
	inStream := buildDataInputStream([]ident.IndexHashBlock{
		{IndexHashes: []ident.IndexHash{idxHash(3), idxHash(4)}},
		{IndexHashes: []ident.IndexHash{idxHash(5)}},
		{IndexHashes: []ident.IndexHash{}},
		{IndexHashes: []ident.IndexHash{idxHash(6)}},
	})
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		ReadMismatch{Type: MismatchError, Err: err},
	})
	defer wg.Wait()

	// NB: this will verify that inStream is drained.
	reportErrorDrainAndClose(err, inStream, outStream)
	assertClosed(t, inStream)
}

func TestDrainRemainingBlockStreamAndClose(t *testing.T) {
	batch := []ident.IndexHash{idxHash(1), idxHash(2)}
	inStream := buildDataInputStream([]ident.IndexHashBlock{
		{IndexHashes: []ident.IndexHash{idxHash(3), idxHash(4)}},
		{IndexHashes: []ident.IndexHash{idxHash(5)}},
		{IndexHashes: []ident.IndexHash{}},
		{IndexHashes: []ident.IndexHash{idxHash(6)}},
	})
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
		mismatch(MismatchOnlyOnPrimary, 3, nil),
		mismatch(MismatchOnlyOnPrimary, 4, nil),
		mismatch(MismatchOnlyOnPrimary, 5, nil),
		mismatch(MismatchOnlyOnPrimary, 6, nil),
	})
	defer wg.Wait()

	drainRemainingBlockStreamAndClose(batch, inStream, outStream)
	assertClosed(t, inStream)
}

func TestReadRemainingReadersAndClose(t *testing.T) {
	entry := idxEntry(1, "bar")
	reader := newEntryReaders(idxEntry(2, "foo"), idxEntry(3, "qux"))
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnSecondary, 1, []byte("bar")),
		mismatch(MismatchOnlyOnSecondary, 2, []byte("foo")),
		mismatch(MismatchOnlyOnSecondary, 3, []byte("qux")),
	})
	defer wg.Wait()

	readRemainingReadersAndClose(entry, reader, outStream)
}

func TestValidate(t *testing.T) {
	foo, bar := []byte("foo"), []byte("bar")
	err := validate(schema.IndexEntry{ID: foo}, foo, 9, 9)
	assert.NoError(t, err)

	err = validate(schema.IndexEntry{ID: foo}, foo, 10, 9)
	assert.Error(t, err)

	err = validate(schema.IndexEntry{ID: foo}, bar, 9, 9)
	assert.NoError(t, err)
}

func TestCompareData(t *testing.T) {
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchData, 2, []byte("def")),
	})

	// checksum match
	compareData(idxEntry(1, "abc"), 1, outStream)
	// checksum mismatch
	compareData(idxEntry(2, "def"), 3, outStream)

	// NB: outStream not closed naturally in this subfunction; close explicitly.
	close(outStream)
	wg.Wait()
}

func TestMoveNextWithRemainingInput(t *testing.T) {
	inStream := buildDataInputStream([]ident.IndexHashBlock{
		{IndexHashes: []ident.IndexHash{idxHash(1), idxHash(2)}},
	})
	reader := newEntryReaders(idxEntry(1, "abc"))
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnPrimary, 1, nil),
		mismatch(MismatchOnlyOnPrimary, 2, nil),
	})
	defer wg.Wait()

	assert.NoError(t, moveNext(inStream, reader, outStream))
	assert.Equal(t, errFinishedStreaming, moveNext(inStream, reader, outStream))
	assertClosed(t, inStream)
}

func TestMoveNextWithExhaustedInput(t *testing.T) {
	inStream := make(chan ident.IndexHashBlock)
	close(inStream)

	reader := newEntryReaders(idxEntry(1, "abc"))
	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{})
	defer wg.Wait()

	assert.NoError(t, moveNext(inStream, reader, outStream))
	assert.Equal(t, errFinishedStreaming, moveNext(inStream, reader, outStream))
}

func TestLoadNextValidIndexHashBlockExhaustedInput(t *testing.T) {
	inStream := make(chan ident.IndexHashBlock)
	close(inStream)

	reader := newEntryReaders(idxEntry(1, "abc"), idxEntry(2, "def"))
	require.True(t, reader.next())

	outStream, wg := buildExpectedOutputStream(t, ReadMismatches{
		mismatch(MismatchOnlyOnSecondary, 1, []byte("abc")),
		mismatch(MismatchOnlyOnSecondary, 2, []byte("def")),
	})
	defer wg.Wait()

	_, err := loadNextValidIndexHashBlock(inStream, reader, outStream)
	assert.Equal(t, errFinishedStreaming, err)
}
