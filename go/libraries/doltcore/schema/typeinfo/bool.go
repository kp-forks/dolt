// Copyright 2020 Dolthub, Inc.
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

package typeinfo

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/types"
)

type boolType struct {
	sqlBitType gmstypes.BitType
}

var _ TypeInfo = (*boolType)(nil)

var BoolType TypeInfo = &boolType{gmstypes.MustCreateBitType(1)}

// ReadFrom reads a go value from a noms types.CodecReader directly

// Equals implements TypeInfo interface.
func (ti *boolType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	_, ok := other.(*boolType)
	return ok
}

// IsValid implements TypeInfo interface.
func (ti *boolType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Bool); ok {
		return true
	}
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}
	return false
}

// NomsKind implements TypeInfo interface.
func (ti *boolType) NomsKind() types.NomsKind {
	return types.BoolKind
}

// Promote implements TypeInfo interface.
func (ti *boolType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *boolType) String() string {
	return "Bool"
}

// ToSqlType implements TypeInfo interface.
func (ti *boolType) ToSqlType() sql.Type {
	return gmstypes.Boolean
}
