// Copyright 2022 Dolthub, Inc.
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

package datas

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

func NewParentsClosure(ctx context.Context, c *Commit, sv types.SerialMessage, vr types.ValueReader, ns tree.NodeStore) (prolly.CommitClosure, error) {
	var msg serial.Commit
	err := serial.InitCommitRoot(&msg, sv, serial.MessagePrefixSz)
	if err != nil {
		return prolly.CommitClosure{}, err
	}
	addr := hash.New(msg.ParentClosureBytes())
	if addr.IsEmpty() {
		return prolly.CommitClosure{}, nil
	}
	v, err := vr.ReadValue(ctx, addr)
	if err != nil {
		return prolly.CommitClosure{}, err
	}
	if types.IsNull(v) {
		return prolly.CommitClosure{}, fmt.Errorf("internal error or data loss: dangling commit parent closure for addr %s for commit %s", addr.String(), c.Addr().String())
	}
	node, fileId, err := tree.NodeFromBytes(v.(types.SerialMessage))
	if err != nil {
		return prolly.CommitClosure{}, err
	}
	if fileId != serial.CommitClosureFileID {
		return prolly.CommitClosure{}, fmt.Errorf("unexpected file ID for commit closure, expected %s, found %s", serial.CommitClosureFileID, fileId)
	}
	return prolly.NewCommitClosure(node, ns)
}

func newParentsClosureIterator(ctx context.Context, c *Commit, vr types.ValueReader, ns tree.NodeStore) (parentsClosureIter, error) {
	sv := c.NomsValue()

	sm := sv.(types.SerialMessage)
	cc, err := NewParentsClosure(ctx, c, sm, vr, ns)
	if err != nil {
		return nil, err
	}
	if cc.IsEmpty() {
		return nil, nil
	}
	ci, err := cc.IterAllReverse(ctx)
	if err != nil {
		return nil, err
	}
	return &fbParentsClosureIterator{i: ci, curr: prolly.NewCommitClosureKey(ns.Pool(), c.Height(), c.Addr()), err: nil}, nil
}

type parentsClosureIter interface {
	Err() error
	Hash() hash.Hash
	Height() uint64
	Less(ctx context.Context, nbf *types.NomsBinFormat, itr parentsClosureIter) bool
	Next(context.Context) bool
}

type fbParentsClosureIterator struct {
	i    prolly.CommitClosureIter
	err  error
	curr prolly.CommitClosureKey
}

func (i *fbParentsClosureIterator) Err() error {
	return i.err
}

func (i *fbParentsClosureIterator) Height() uint64 {
	if i.err != nil {
		return 0
	}
	return i.curr.Height()
}

func (i *fbParentsClosureIterator) Hash() hash.Hash {
	if i.err != nil {
		return hash.Hash{}
	}
	return i.curr.Addr()
}

func (i *fbParentsClosureIterator) Next(ctx context.Context) bool {
	if i.err != nil {
		return false
	}
	i.curr, _, i.err = i.i.Next(ctx)
	if i.err == io.EOF {
		i.err = nil
		return false
	}
	return true
}

func (i *fbParentsClosureIterator) Less(ctx context.Context, nbf *types.NomsBinFormat, otherI parentsClosureIter) bool {
	other := otherI.(*fbParentsClosureIterator)
	return i.curr.Less(ctx, other.curr)
}

func writeTypesCommitParentClosure(ctx context.Context, vrw types.ValueReadWriter, parentRefsL types.List) (types.Ref, bool, error) {
	parentRefs := make([]types.Ref, int(parentRefsL.Len()))
	parents := make([]types.Struct, len(parentRefs))
	if len(parents) == 0 {
		return types.Ref{}, false, nil
	}
	err := parentRefsL.IterAll(ctx, func(v types.Value, i uint64) error {
		r, ok := v.(types.Ref)
		if !ok {
			return errors.New("parentsRef element was not a Ref")
		}
		parentRefs[int(i)] = r
		tv, err := r.TargetValue(ctx, vrw)
		if err != nil {
			return err
		}
		s, ok := tv.(types.Struct)
		if !ok {
			return errors.New("parentRef target value was not a Struct")
		}
		parents[int(i)] = s
		return nil
	})
	if err != nil {
		return types.Ref{}, false, err
	}
	parentMaps := make([]types.Map, len(parents))
	parentParentLists := make([]types.List, len(parents))
	for i, p := range parents {
		v, ok, err := p.MaybeGet(parentsClosureField)
		if err != nil {
			return types.Ref{}, false, err
		}
		if !ok || types.IsNull(v) {
			empty, err := types.NewMap(ctx, vrw)
			if err != nil {
				return types.Ref{}, false, err
			}
			parentMaps[i] = empty
		} else {
			r, ok := v.(types.Ref)
			if !ok {
				return types.Ref{}, false, errors.New("unexpected field value type for parents_closure in commit struct")
			}
			tv, err := r.TargetValue(ctx, vrw)
			if err != nil {
				return types.Ref{}, false, err
			}
			parentMaps[i], ok = tv.(types.Map)
			if !ok {
				return types.Ref{}, false, fmt.Errorf("unexpected target value type for parents_closure in commit struct: %v", tv)
			}
		}
		v, ok, err = p.MaybeGet(parentsListField)
		if err != nil {
			return types.Ref{}, false, err
		}
		if !ok || types.IsNull(v) {
			empty, err := types.NewList(ctx, vrw)
			if err != nil {
				return types.Ref{}, false, err
			}
			parentParentLists[i] = empty
		} else {
			parentParentLists[i], ok = v.(types.List)
			if !ok {
				return types.Ref{}, false, errors.New("unexpected field value or type for parents_list in commit struct")
			}
		}
		if parentMaps[i].Len() == 0 && parentParentLists[i].Len() != 0 {
			// If one of the commits has an empty parents_closure, but non-empty parents, we will not record
			// a parents_closure here.
			return types.Ref{}, false, nil
		}
	}
	// Convert parent lists to List<Ref<Value>>
	for i, l := range parentParentLists {
		newRefs := make([]types.Value, int(l.Len()))
		err := l.IterAll(ctx, func(v types.Value, i uint64) error {
			r, ok := v.(types.Ref)
			if !ok {
				return errors.New("unexpected entry type for parents_list in commit struct")
			}
			newRefs[int(i)], err = types.ToRefOfValue(r, vrw.Format())
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return types.Ref{}, false, err
		}
		parentParentLists[i], err = types.NewList(ctx, vrw, newRefs...)
		if err != nil {
			return types.Ref{}, false, err
		}
	}
	editor := parentMaps[0].Edit()
	for i, r := range parentRefs {
		h := r.TargetHash()
		key, err := types.NewTuple(vrw.Format(), types.Uint(r.Height()), types.InlineBlob(h[:]))
		if err != nil {
			editor.Close(ctx)
			return types.Ref{}, false, err
		}
		editor.Set(key, parentParentLists[i])
	}
	for i := 1; i < len(parentMaps); i++ {
		changes := make(chan types.ValueChanged)
		var derr error
		go func() {
			defer close(changes)
			derr = parentMaps[1].Diff(ctx, parentMaps[0], changes)
		}()
		for c := range changes {
			if c.ChangeType == types.DiffChangeAdded {
				editor.Set(c.Key, c.NewValue)
			}
		}
		if derr != nil {
			editor.Close(ctx)
			return types.Ref{}, false, derr
		}
	}
	m, err := editor.Map(ctx)
	if err != nil {
		return types.Ref{}, false, err
	}
	r, err := vrw.WriteValue(ctx, m)
	if err != nil {
		return types.Ref{}, false, err
	}
	r, err = types.ToRefOfValue(r, vrw.Format())
	if err != nil {
		return types.Ref{}, false, err
	}
	return r, true, nil
}

func writeFbCommitParentClosure(ctx context.Context, cs chunks.ChunkStore, vrw types.ValueReadWriter, ns tree.NodeStore, parents []*serial.Commit, parentAddrs []hash.Hash) (hash.Hash, error) {
	if len(parents) == 0 {
		// We write an empty hash for parent-less commits of height 1.
		return hash.Hash{}, nil
	}
	// Fetch the parent closures of our parents.
	addrs := make([]hash.Hash, len(parents))
	for i := range parents {
		addrs[i] = hash.New(parents[i].ParentClosureBytes())
	}
	vs, err := vrw.ReadManyValues(ctx, addrs)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: ReadManyValues: %w", err)
	}
	// Load them as ProllyTrees.
	closures := make([]prolly.CommitClosure, len(parents))
	for i := range addrs {
		if !types.IsNull(vs[i]) {
			node, fileId, err := tree.NodeFromBytes(vs[i].(types.SerialMessage))
			if err != nil {
				return hash.Hash{}, err
			}
			if fileId != serial.CommitClosureFileID {
				return hash.Hash{}, fmt.Errorf("unexpected file ID for commit closure, expected %s, found %s", serial.CommitClosureFileID, fileId)
			}
			closures[i], err = prolly.NewCommitClosure(node, ns)
			if err != nil {
				return hash.Hash{}, err
			}
		} else {
			closures[i], err = prolly.NewEmptyCommitClosure(ns)
			if err != nil {
				return hash.Hash{}, err
			}
		}
	}
	// Add all the missing entries from [1, ...) maps to the 0th map.
	editor := closures[0].Editor()
	for i := 1; i < len(closures); i++ {
		err = prolly.DiffCommitClosures(ctx, closures[0], closures[i], func(ctx context.Context, diff tree.Diff) error {
			if diff.Type == tree.AddedDiff {
				return editor.Add(ctx, prolly.CommitClosureKey(diff.Key))
			}
			return nil
		})
		if err != nil && !errors.Is(err, io.EOF) {
			return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: DiffCommitClosures: %w", err)
		}
	}
	// Add the parents themselves to the new map.
	for i := 0; i < len(parents); i++ {
		err = editor.Add(ctx, prolly.NewCommitClosureKey(ns.Pool(), parents[i].Height(), parentAddrs[i]))
		if err != nil {
			return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: MutableCommitClosure.Put: %w", err)
		}
	}
	// This puts the closure in the NodeStore as well.
	res, err := editor.Flush(ctx)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("writeCommitParentClosure: MutableCommitClosure.Flush: %w", err)
	}
	return res.HashOf(), nil
}
