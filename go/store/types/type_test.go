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

func TestTypes(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	mapType, err := MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])
	require.NoError(t, err)
	setType, err := MakeSetType(PrimitiveTypeMap[StringKind])
	require.NoError(t, err)
	mahType, err := MakeStructType("MahStruct",
		StructField{PrimitiveTypeMap[StringKind], "Field1", false},
		StructField{PrimitiveTypeMap[BoolKind], "Field2", false},
	)
	require.NoError(t, err)
	recType, err := MakeStructType("RecursiveStruct", StructField{MakeCycleType("RecursiveStruct"), "self", false})
	require.NoError(t, err)

	mRef := mustRef(vs.WriteValue(context.Background(), mapType)).TargetHash()
	setRef := mustRef(vs.WriteValue(context.Background(), setType)).TargetHash()
	mahRef := mustRef(vs.WriteValue(context.Background(), mahType)).TargetHash()
	recRef := mustRef(vs.WriteValue(context.Background(), recType)).TargetHash()

	assert.True(mapType.Equals(mustValue(vs.ReadValue(context.Background(), mRef))))
	assert.True(setType.Equals(mustValue(vs.ReadValue(context.Background(), setRef))))
	assert.True(mahType.Equals(mustValue(vs.ReadValue(context.Background(), mahRef))))
	assert.True(recType.Equals(mustValue(vs.ReadValue(context.Background(), recRef))))
}

func TestTypeType(t *testing.T) {
	assert.True(t, mustType(TypeOf(PrimitiveTypeMap[BoolKind])).Equals(PrimitiveTypeMap[TypeKind]))
}

func TestTypeRefDescribe(t *testing.T) {
	assert := assert.New(t)
	mapType, err := MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[FloatKind])
	require.NoError(t, err)
	setType, err := MakeSetType(PrimitiveTypeMap[StringKind])
	require.NoError(t, err)

	assert.Equal("Bool", mustString(PrimitiveTypeMap[BoolKind].Describe(context.Background())))
	assert.Equal("Float", mustString(PrimitiveTypeMap[FloatKind].Describe(context.Background())))
	assert.Equal("String", mustString(PrimitiveTypeMap[StringKind].Describe(context.Background())))
	assert.Equal("UUID", mustString(PrimitiveTypeMap[UUIDKind].Describe(context.Background())))
	assert.Equal("Int", mustString(PrimitiveTypeMap[IntKind].Describe(context.Background())))
	assert.Equal("Uint", mustString(PrimitiveTypeMap[UintKind].Describe(context.Background())))
	assert.Equal("InlineBlob", mustString(PrimitiveTypeMap[InlineBlobKind].Describe(context.Background())))
	assert.Equal("Decimal", mustString(PrimitiveTypeMap[DecimalKind].Describe(context.Background())))
	assert.Equal("Map<String, Float>", mustString(mapType.Describe(context.Background())))
	assert.Equal("Set<String>", mustString(setType.Describe(context.Background())))

	mahType, err := MakeStructType("MahStruct",
		StructField{PrimitiveTypeMap[StringKind], "Field1", false},
		StructField{PrimitiveTypeMap[BoolKind], "Field2", false},
	)
	require.NoError(t, err)
	assert.Equal("Struct MahStruct {\n  Field1: String,\n  Field2: Bool,\n}", mustString(mahType.Describe(context.Background())))
}

func TestTypeOrdered(t *testing.T) {
	assert := assert.New(t)
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[BoolKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[FloatKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[UUIDKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[StringKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[IntKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[UintKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[InlineBlobKind].TargetKind()))
	assert.True(isKindOrderedByValue(PrimitiveTypeMap[DecimalKind].TargetKind()))
	assert.True(isKindOrderedByValue(TupleKind))

	assert.False(isKindOrderedByValue(PrimitiveTypeMap[BlobKind].TargetKind()))
	assert.False(isKindOrderedByValue(PrimitiveTypeMap[ValueKind].TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeListType(PrimitiveTypeMap[StringKind])).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeSetType(PrimitiveTypeMap[StringKind])).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeMapType(PrimitiveTypeMap[StringKind], PrimitiveTypeMap[ValueKind])).TargetKind()))
	assert.False(isKindOrderedByValue(mustType(MakeRefType(PrimitiveTypeMap[StringKind])).TargetKind()))
}
