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

package row

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/types"
)

type TaggedValues map[uint64]types.Value

type TupleVals struct {
	vs  []types.Value
	nbf *types.NomsBinFormat
}

func (tvs TupleVals) Kind() types.NomsKind {
	return types.TupleKind
}

func (tvs TupleVals) Value(ctx context.Context) (types.Value, error) {
	return types.NewTuple(tvs.nbf, tvs.vs...)
}

func (tvs TupleVals) Less(ctx context.Context, nbf *types.NomsBinFormat, other types.LesserValuable) (bool, error) {
	if other.Kind() == types.TupleKind {
		if otherTVs, ok := other.(TupleVals); ok {
			for i, val := range tvs.vs {
				if i == len(otherTVs.vs) {
					// equal up til the end of other. other is shorter, therefore it is less
					return false, nil
				}

				otherVal := otherTVs.vs[i]

				if !val.Equals(otherVal) {
					return val.Less(ctx, nbf, otherVal)
				}
			}

			return len(tvs.vs) < len(otherTVs.vs), nil
		} else {
			panic("not supported")
		}
	}

	return types.TupleKind < other.Kind(), nil
}

func (tt TaggedValues) nomsTupleForTags(nbf *types.NomsBinFormat, tags []uint64, encodeNulls bool) TupleVals {
	vals := make([]types.Value, 0, 2*len(tags))
	for _, tag := range tags {
		val := tt[tag]

		if types.IsNull(val) && !encodeNulls {
			continue
		} else if val == nil {
			val = types.NullValue
		}

		vals = append(vals, types.Uint(tag))
		vals = append(vals, val)
	}

	return TupleVals{vals, nbf}
}

func (tt TaggedValues) Iter(cb func(tag uint64, val types.Value) (stop bool, err error)) (bool, error) {
	stop := false

	var err error
	for tag, val := range tt {
		stop, err = cb(tag, val)

		if stop || err != nil {
			break
		}
	}

	return stop, err
}

func (tt TaggedValues) Get(tag uint64) (types.Value, bool) {
	val, ok := tt[tag]
	return val, ok
}

func (tt TaggedValues) String() string {
	str := "{"
	for k, v := range tt {
		encStr, err := types.EncodedValue(context.Background(), v)

		if err != nil {
			return err.Error()
		}

		str += fmt.Sprintf("\n\t%d: %s", k, encStr)
	}

	str += "\n}"
	return str
}
