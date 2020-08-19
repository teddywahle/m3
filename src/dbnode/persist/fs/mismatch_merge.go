// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software withoutStream restriction, including withoutStream limitation the rights
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
	"bytes"
	"errors"
	"fmt"

	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/ident"
)

var errFinishedStreaming = errors.New("mismatch_streaming_finished")

type entry struct {
	idHash uint64
	entry  schema.IndexEntry
}

func (e entry) toMismatch(t MismatchType) ReadMismatch {
	return ReadMismatch{
		Type:     t,
		Checksum: uint32(e.entry.DataChecksum),
		IDHash:   e.idHash,
		Data:     nil,                       // TODO: add these correctly.
		Tags:     nil,                       // TODO: add these correctly.
		ID:       ident.BytesID(e.entry.ID), // TODO: pool these correctly.
	}
}

type entryReader interface {
	next() bool
	current() entry
}

func reportErrorDrainAndClose(
	err error,
	inStream <-chan ident.IndexHashBlock,
	outStream chan<- ReadMismatch,
) {
	outStream <- ReadMismatch{Type: MismatchError, Err: err}
	close(outStream)
	for range inStream {
		// no-op, drain input stream.
	}
}

func drainRemainingBlockStreamAndClose(
	currentBatch []ident.IndexHash,
	inStream <-chan ident.IndexHashBlock,
	outStream chan<- ReadMismatch,
) {
	drain := func(hashes []ident.IndexHash) {
		for _, c := range hashes {
			outStream <- ReadMismatch{
				Type:     MismatchOnlyOnPrimary,
				Checksum: c.DataChecksum,
				IDHash:   c.IDHash,
			}
		}
	}

	drain(currentBatch)

	for batch := range inStream {
		drain(batch.IndexHashes)
	}

	close(outStream)
}

func readRemainingReadersAndClose(
	current entry,
	reader entryReader,
	outStream chan<- ReadMismatch,
) {
	outStream <- current.toMismatch(MismatchOnlyOnSecondary)
	for reader.next() {
		outStream <- reader.current().toMismatch(MismatchOnlyOnSecondary)
	}

	close(outStream)
}

func validate(c schema.IndexEntry, marker []byte, id, hID uint64) error {
	// TODO: use a proper invariant here
	if hID != id {
		return fmt.Errorf("invariant error, hashes for %s(%d) and %s(%d) mismatch",
			string(c.ID), id, string(marker), hID)
	}
	return nil
}

func compareData(e entry, dataChecksum uint32, outStream chan<- ReadMismatch) {
	// NB: If data checksums match, this entry matches.
	if dataChecksum == uint32(e.entry.DataChecksum) {
		return
	}

	// Mark current entry as DATA_MISMATCH if there is a data mismatch.
	outStream <- e.toMismatch(MismatchData)
}

func loadNextValidIndexHashBlock(
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch) (ident.IndexHashBlock, error) {
	var (
		batch ident.IndexHashBlock
		ok    bool
	)

	curr := r.current()
	for {
		batch, ok = <-inStream
		if !ok {
			// NB: finished streaming from hash block. Mark remaining entries as
			// ONLY_SECONDARY and return.
			readRemainingReadersAndClose(curr, r, outStream)
			return ident.IndexHashBlock{}, errFinishedStreaming
		}

		if compare := bytes.Compare(batch.Marker, curr.entry.ID); compare < 0 {
			// NB: current element is before the current MARKER element;
			// this is a valid index hash block for comparison.
			return batch, nil
		} else if compare == 0 {
			// NB: edge case: the last (i.e. MARKER) element is the first one in the
			// index batch to match the current element.
			lastIdx := len(batch.IndexHashes) - 1

			// NB: sanity check that matching IDs <=> matching ID hash.
			if err := validate(
				curr.entry, batch.Marker, curr.idHash,
				batch.IndexHashes[lastIdx].IDHash,
			); err != nil {
				return ident.IndexHashBlock{}, err
			}

			for i, idxHash := range batch.IndexHashes {
				// NB: Mark all preceeding entries as ONLY_PRIMARY mismatches.
				if lastIdx != i {
					outStream <- ReadMismatch{
						Type:     MismatchOnlyOnPrimary,
						Checksum: idxHash.DataChecksum,
						IDHash:   idxHash.IDHash,
					}

					continue
				}

				compareData(curr, idxHash.DataChecksum, outStream)
			}

			// Finished iterating through entry reader, drain any remaining entries
			// and return.
			if !r.next() {
				drainRemainingBlockStreamAndClose(batch.IndexHashes, inStream, outStream)
				return ident.IndexHashBlock{}, errFinishedStreaming
			}

			curr = r.current()
		}

		// NB: all elements from the current idxHashBatch are exhausted; wait
		// for the next element to be streamed in and check to see if the new
		// block is valid to read from.
	}
}

func moveNext(
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch) error {
	if !r.next() {
		// NB: no values in the entry reader
		batch, ok := <-inStream
		if ok {
			// NB: drain the input stream as fully ONLY_PRIMARY mismatches.
			drainRemainingBlockStreamAndClose(batch.IndexHashes, inStream, outStream)
		} else {
			// NB: no values in the input stream either, close the output stream.
			close(outStream)
		}

		return errFinishedStreaming
	}

	return nil
}

func mergeHelper(
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch) error {
	if err := moveNext(inStream, r, outStream); err != nil {
		return err
	}

	batch, err := loadNextValidIndexHashBlock(inStream, r, outStream)
	if err != nil {
		return err
	}

	batchIdx := 0
	markerIdx := len(batch.IndexHashes) - 1

	for {
		entry := r.current()
		hash := batch.IndexHashes[batchIdx]

		// NB: this is the last element in the batch. Check against MARKER.
		if batchIdx == markerIdx {
			if entry.idHash == hash.IDHash {
				// NB: sanity check that matching IDs <=> matching ID hash.
				if err := validate(
					entry.entry, batch.Marker, entry.idHash, hash.IDHash,
				); err != nil {
					return err
				}

				compareData(entry, hash.DataChecksum, outStream)

				// NB: get next reader element.
				if err := moveNext(inStream, r, outStream); err != nil {
					return err
				}

				// NB: get next index hash block, and reset batch and marker indices.
				batch, err = loadNextValidIndexHashBlock(inStream, r, outStream)
				if err != nil {
					return err
				}

				batchIdx = 0
				markerIdx = len(batch.IndexHashes) - 1
				continue
			}

			// NB: compare marker ID with current ID.
			compare := bytes.Compare(batch.Marker, entry.entry.ID)
			if compare == 0 {
				// NB: this is an error since hashed IDs mismatch here.
				return validate(entry.entry, batch.Marker, entry.idHash, hash.IDHash)
			} else if compare > 0 {
				// NB: current entry ID is before marker ID; mark the current entry
				// as a ONLY_SECONDARY mismatch and move to next reader element.
				outStream <- entry.toMismatch(MismatchOnlyOnSecondary)
				if err := moveNext(inStream, r, outStream); err != nil {
					return err
				}

				continue
			}

			// NB: get next index hash block, and reset batch and marker indices.
			batch, err = loadNextValidIndexHashBlock(inStream, r, outStream)
			if err != nil {
				return err
			}

			batchIdx = 0
			markerIdx = len(batch.IndexHashes) - 1
		}

		if entry.idHash == hash.IDHash {
			batchIdx++
			compareData(entry, hash.DataChecksum, outStream)
			// NB: try to move next entry and next batch item.
			if !r.next() {
				remaining := batch.IndexHashes[batchIdx:]
				drainRemainingBlockStreamAndClose(remaining, inStream, outStream)
				return nil
			}

			// NB: move to next element.
			continue
		}

		nextBatchIdx := batchIdx + 1
		for ; nextBatchIdx < markerIdx; nextBatchIdx++ {
			// NB: read next hashes, checking for index checksum matches.
			nextHash := batch.IndexHashes[nextBatchIdx]
			if entry.idHash == nextHash.IDHash {
				// NB: found matching checksum; add all indexHash entries between
				// batchIdx and nextBatchIdx as ONLY_PRIMARY mismatches.
				for _, c := range batch.IndexHashes[batchIdx:nextBatchIdx] {
					outStream <- ReadMismatch{
						Type:     MismatchOnlyOnPrimary,
						Checksum: c.DataChecksum,
						IDHash:   c.IDHash,
					}
				}

				batchIdx = nextBatchIdx + 1
				compareData(entry, nextHash.DataChecksum, outStream)
				// NB: try to move next entry and next batch item.
				if !r.next() {
					remaining := batch.IndexHashes[batchIdx:]
					drainRemainingBlockStreamAndClose(remaining, inStream, outStream)
					return nil
				}

				// NB: move to next element.
				continue
			}
		}

		// NB: reached MATCHER point in the batch.
		nextHash := batch.IndexHashes[markerIdx]
		if entry.idHash == nextHash.IDHash {
			// NB: sanity check that matching IDs <=> matching ID hash.
			if err := validate(
				entry.entry, batch.Marker, entry.idHash, nextHash.IDHash,
			); err != nil {
				return err
			}

			// NB: Mark remaining entries in the batch as ONLY_PRIMARY mismatches.
			for _, c := range batch.IndexHashes[batchIdx:markerIdx] {
				outStream <- ReadMismatch{
					Type:     MismatchOnlyOnPrimary,
					Checksum: c.DataChecksum,
					IDHash:   c.IDHash,
				}
			}

			compareData(entry, nextHash.DataChecksum, outStream)

			// NB: get next reader element.
			if err := moveNext(inStream, r, outStream); err != nil {
				return err
			}

			// NB: get next index hash block, and reset batch and marker indices.
			batch, err = loadNextValidIndexHashBlock(inStream, r, outStream)
			if err != nil {
				return err
			}

			batchIdx = 0
			markerIdx = len(batch.IndexHashes) - 1
			continue
		}

		// NB: compare marker ID with current ID.
		compare := bytes.Compare(batch.Marker, entry.entry.ID)
		if compare == 0 {
			// NB: this is an error since hashed IDs mismatch here.
			return validate(entry.entry, batch.Marker, entry.idHash, nextHash.IDHash)
		} else if compare > 0 {
			// NB: current entry ID is before marker ID; mark the current entry
			// as a ONLY_SECONDARY mismatch and move to next reader element.
			outStream <- entry.toMismatch(MismatchOnlyOnSecondary)
			if err := moveNext(inStream, r, outStream); err != nil {
				return err
			}

			continue
		}

		// NB: current entry ID is past marker ID; mark any remaining elements in
		// current batch as ONLY_PRIMARY, and increment batch.
		for _, c := range batch.IndexHashes[batchIdx:] {
			outStream <- ReadMismatch{
				Type:     MismatchOnlyOnPrimary,
				Checksum: c.DataChecksum,
				IDHash:   c.IDHash,
			}
		}

		// NB: get next index hash block, and reset batch and marker indices.
		batch, err = loadNextValidIndexHashBlock(inStream, r, outStream)
		if err != nil {
			return err
		}

		batchIdx = 0
		markerIdx = len(batch.IndexHashes) - 1
	}
}

func merge(
	inStream <-chan ident.IndexHashBlock,
	r entryReader,
	outStream chan<- ReadMismatch) error {
	err := mergeHelper(inStream, r, outStream)
	if err == nil {
		return nil
	}

	if err == errFinishedStreaming {
		return nil
	}

	reportErrorDrainAndClose(err, inStream, outStream)
	return err
}
