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
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReverseMapIterator(t *testing.T) {
	ctx := context.Background()
	vrw := newTestValueStore()
	m, err := NewMap(ctx, vrw)
	require.NoError(t, err)

	me := m.Edit()
	for i := 0; i <= 100; i += 2 {
		me.Set(Int(i), Int(100-i))
	}

	m, err = me.Map(context.Background())
	require.NoError(t, err)

	test := func(start, expected int, name string) {
		t.Run(name, func(t *testing.T) {
			it, err := m.IteratorBackFrom(context.Background(), Int(start))
			require.NoError(t, err)

			expectedItemIterCount := (expected / 2) + 1
			var valsIteratedOver int

			for {
				k, v, err := it.Next(ctx)
				require.NoError(t, err)

				if k == nil {
					break
				}

				kn, vn := int(k.(Int)), int(v.(Int))

				assert.Equal(t, expected, kn)
				assert.Equal(t, 100-kn, vn)

				expected = kn - 2
				valsIteratedOver++
			}

			if start < 0 {
				assert.Equal(t, valsIteratedOver, 0)
			} else {
				assert.Equal(t, expected, -2)
				assert.Equal(t, valsIteratedOver, expectedItemIterCount)
			}
		})

	}

	test(100, 100, "Iterate in reverse from end")
	test(200, 100, "Iterate in reverse from beyond the end")
	test(50, 50, "Iterate in reverse from the middle")
	test(51, 50, "Iterate in reverse from a key not in the map")
	test(0, 0, "Iterate in reverse from the first key")
	test(-1, 0, "Iterate in reverse from before the first day")
}
