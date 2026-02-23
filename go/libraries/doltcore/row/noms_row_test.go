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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	lnColName       = "last"
	fnColName       = "first"
	addrColName     = "address"
	ageColName      = "age"
	titleColName    = "title"
	reservedColName = "reserved"
	indexName       = "idx_age"
	lnColTag        = 1
	fnColTag        = 0
	addrColTag      = 6
	ageColTag       = 4
	titleColTag     = 40
	reservedColTag  = 50
	unusedTag       = 100
)

var lnVal = types.String("astley")
var fnVal = types.String("rick")
var addrVal = types.String("123 Fake St")
var ageVal = types.Uint(53)
var titleVal = types.NullValue

var testKeyCols = []schema.Column{
	{Name: lnColName, Tag: lnColTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: []schema.ColConstraint{schema.NotNullConstraint{}}},
	{Name: fnColName, Tag: fnColTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: []schema.ColConstraint{schema.NotNullConstraint{}}},
}
var testCols = []schema.Column{
	{Name: addrColName, Tag: addrColTag, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	{Name: ageColName, Tag: ageColTag, Kind: types.UintKind, IsPartOfPK: false, TypeInfo: typeinfo.Uint64Type, Constraints: nil},
	{Name: titleColName, Tag: titleColTag, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	{Name: reservedColName, Tag: reservedColTag, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
}
var testKeyColColl = schema.NewColCollection(testKeyCols...)
var testNonKeyColColl = schema.NewColCollection(testCols...)
var sch, _ = schema.SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColColl)
var index schema.Index

func init() {
	index, _ = sch.Indexes().AddIndexByColTags(indexName, []uint64{ageColTag}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
}

func newTestRow() (Row, error) {
	vals := TaggedValues{
		fnColTag:    fnVal,
		lnColTag:    lnVal,
		addrColTag:  addrVal,
		ageColTag:   ageVal,
		titleColTag: titleVal,
	}

	return New(types.Format_Default, sch, vals)
}

func TestItrRowCols(t *testing.T) {
	r, err := newTestRow()
	require.NoError(t, err)

	itrVals := make(TaggedValues)
	_, err = r.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		itrVals[tag] = val
		return false, nil
	})
	require.NoError(t, err)

	assert.Equal(t, TaggedValues{
		lnColTag:    lnVal,
		fnColTag:    fnVal,
		ageColTag:   ageVal,
		addrColTag:  addrVal,
		titleColTag: titleVal,
	}, itrVals)
}

func TestSetColVal(t *testing.T) {
	t.Run("valid update", func(t *testing.T) {
		expected := map[uint64]types.Value{
			lnColTag:    lnVal,
			fnColTag:    fnVal,
			ageColTag:   ageVal,
			addrColTag:  addrVal,
			titleColTag: titleVal}

		updatedVal := types.String("sanchez")

		r, err := newTestRow()
		require.NoError(t, err)
		r2, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r2)

		updated, err := r.SetColVal(lnColTag, updatedVal, sch)
		require.NoError(t, err)

		// validate calling set does not mutate the original row
		r3, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r3)
		expected[lnColTag] = updatedVal
		r4, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, updated, r4)

		// set to a nil value
		updated, err = updated.SetColVal(titleColTag, nil, sch)
		require.NoError(t, err)
		delete(expected, titleColTag)
		r5, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, updated, r5)
	})

	t.Run("invalid update", func(t *testing.T) {
		expected := map[uint64]types.Value{
			lnColTag:    lnVal,
			fnColTag:    fnVal,
			ageColTag:   ageVal,
			addrColTag:  addrVal,
			titleColTag: titleVal}

		r, err := newTestRow()
		require.NoError(t, err)

		r2, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r2)

		// SetColVal allows an incorrect type to be set for a column
		updatedRow, err := r.SetColVal(lnColTag, types.Bool(true), sch)
		require.NoError(t, err)
		// IsValid fails for the type problem
		isv, err := IsValid(updatedRow, sch)
		require.NoError(t, err)
		assert.False(t, isv)
		invalidCol, err := GetInvalidCol(updatedRow, sch)
		require.NoError(t, err)
		assert.NotNil(t, invalidCol)
		assert.Equal(t, uint64(lnColTag), invalidCol.Tag)

		// validate calling set does not mutate the original row
		r3, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r3)
	})
}

func TestReduceToIndex(t *testing.T) {
	taggedValues := []struct {
		row           TaggedValues
		expectedIndex TaggedValues
	}{
		{
			TaggedValues{
				lnColTag:       types.String("yes"),
				fnColTag:       types.String("no"),
				addrColTag:     types.String("nonsense"),
				ageColTag:      types.Uint(55),
				titleColTag:    types.String("lol"),
				reservedColTag: types.String("what"),
			},
			TaggedValues{
				lnColTag:  types.String("yes"),
				fnColTag:  types.String("no"),
				ageColTag: types.Uint(55),
			},
		},
		{
			TaggedValues{
				lnColTag:       types.String("yes"),
				addrColTag:     types.String("nonsense"),
				ageColTag:      types.Uint(55),
				titleColTag:    types.String("lol"),
				reservedColTag: types.String("what"),
			},
			TaggedValues{
				lnColTag:  types.String("yes"),
				ageColTag: types.Uint(55),
			},
		},
		{
			TaggedValues{
				lnColTag: types.String("yes"),
				fnColTag: types.String("no"),
			},
			TaggedValues{
				lnColTag: types.String("yes"),
				fnColTag: types.String("no"),
			},
		},
		{
			TaggedValues{
				addrColTag:     types.String("nonsense"),
				titleColTag:    types.String("lol"),
				reservedColTag: types.String("what"),
			},
			TaggedValues{},
		},
	}

	for _, tvCombo := range taggedValues {
		row, err := New(types.Format_Default, sch, tvCombo.row)
		require.NoError(t, err)
		expectedIndex, err := New(types.Format_Default, index.Schema(), tvCombo.expectedIndex)
		require.NoError(t, err)
		indexRow, err := reduceToIndex(index, row)
		require.NoError(t, err)
		assert.True(t, AreEqual(expectedIndex, indexRow, index.Schema()))
	}
}

func reduceToIndex(idx schema.Index, r Row) (Row, error) {
	newRow := nomsRow{
		key:   make(TaggedValues),
		value: make(TaggedValues),
		nbf:   r.Format(),
	}
	for _, tag := range idx.AllTags() {
		if val, ok := r.GetColVal(tag); ok {
			newRow.key[tag] = val
		}
	}

	return newRow, nil
}
