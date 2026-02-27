// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"context"
	"io"
)

// EditProvider is an interface which provides map edits as KVPs where each edit is a key and the new value
// associated with the key for inserts and updates.  deletes are modeled as a key with no value
type EditProvider interface {
	// Next returns the next KVP representing the next edit to be applied.  Next will always return KVPs
	// in key sorted order.  Once all KVPs have been read io.EOF will be returned.
	Next(ctx context.Context) (*KVP, error)

	// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
	// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
	// been read
	ReachedEOF() bool

	Close(ctx context.Context) error
}

// EmptyEditProvider is an EditProvider implementation that has no edits
type EmptyEditProvider struct{}

// Next will always return nil, io.EOF
func (eep EmptyEditProvider) Next(ctx context.Context) (*KVP, error) {
	return nil, io.EOF
}

// ReachedEOF returns true once all data is exhausted.  If ReachedEOF returns false that does not mean that there
// is more data, only that io.EOF has not been returned previously.  If ReachedEOF returns true then all edits have
// been read
func (eep EmptyEditProvider) ReachedEOF() bool {
	return true
}

func (eep EmptyEditProvider) Close(ctx context.Context) error {
	return nil
}

// Before edits can be applied th cursor position for each edit must be found.  mapWork represents a piece of work to be
// done by the worker threads which are executing the prepWorker function.  Each piece of work will be a batch of edits
// whose cursor needs to be found, and a chan where results should be written.
type mapWork struct {
	resChan chan mapWorkResult
	kvps    []*KVP
}

// mapWorkResult is the result of a single mapWork instance being processed.
type mapWorkResult struct {
	seqCurs       []*sequenceCursor
	cursorEntries [][]mapEntry
}

const (
	workerCount = 7

	// batch sizes start small in order to get the sequenceChunker work to do quickly.  Batches will grow to a maximum
	// size at a given multiplier
	batchSizeStart = 10
	batchMult      = 1.25
	batchSizeMax   = 5000
)

// AppliedEditStats contains statistics on what edits were applied in types.ApplyEdits
type AppliedEditStats struct {
	// Additions counts the number of elements added to the map
	Additions int64

	// Modifications counts the number of map entries that were modified
	Modifications int64

	// SamVal counts the number of edits that had no impact because a value was set to the same value that is already
	// stored in the map
	SameVal int64

	// Deletions counts the number of items deleted from the map
	Deletions int64

	// NonexistentDeletes counts the number of items where a deletion was attempted, but the key didn't exist in the map
	// so there was no impact
	NonExistentDeletes int64
}
