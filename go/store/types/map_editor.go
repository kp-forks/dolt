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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
)

// EditAccumulator is an interface for a datastructure that can have edits added to it. Once all edits are
// added FinishedEditing can be called to get an EditProvider which provides the edits in sorted order
type EditAccumulator interface {
	// EditsAdded returns the number of edits that have been added to this EditAccumulator
	EditsAdded() int

	// AddEdit adds an edit to the list of edits.  Not thread safe.
	AddEdit(k LesserValuable, v Valuable)

	// FinishedEditing should be called when all edits have been added to get an EditProvider which provides the
	// edits in sorted order. Adding more edits after calling FinishedEditing is an error.
	FinishedEditing(context.Context) (EditProvider, error)

	// Close ensures that the accumulator is closed. Repeat calls are allowed. Not guaranteed to be thread-safe, thus
	// requires external synchronization.
	Close(context.Context) error
}
