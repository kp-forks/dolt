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

// This is a dolt implementation of the MySQL type Geometry, thus most of the functionality
// within is directly reliant on the go-mysql-server implementation.
type geometryType struct {
	sqlGeometryType gmstypes.GeometryType // References the corresponding GeometryType in GMS
}

var _ TypeInfo = (*geometryType)(nil)

var GeometryType = &geometryType{gmstypes.GeometryType{}}

// ReadFrom reads a go value from a noms types.CodecReader directly
func (ti *geometryType) ReadFrom(nbf *types.NomsBinFormat, reader types.CodecReader) (interface{}, error) {
	k := reader.ReadKind()
	switch k {
	case types.PointKind:
		val, err := reader.ReadPoint()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesPointToSQLPoint(val), nil
	case types.LineStringKind:
		val, err := reader.ReadLineString()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesLineStringToSQLLineString(val), nil
	case types.PolygonKind:
		val, err := reader.ReadPolygon()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesPolygonToSQLPolygon(val), nil
	case types.MultiPointKind:
		val, err := reader.ReadMultiPoint()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesMultiPointToSQLMultiPoint(val), nil
	case types.MultiLineStringKind:
		val, err := reader.ReadMultiLineString()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesMultiLineStringToSQLMultiLineString(val), nil
	case types.MultiPolygonKind:
		val, err := reader.ReadMultiPolygon()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesMultiPolygonToSQLMultiPolygon(val), nil
	case types.GeometryCollectionKind:
		val, err := reader.ReadGeomColl()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesGeomCollToSQLGeomColl(val), nil
	case types.GeometryKind:
		// Note: GeometryKind is no longer written
		// included here for backward compatibility
		val, err := reader.ReadGeometry()
		if err != nil {
			return nil, err
		}
		return types.ConvertTypesGeometryToSQLGeometry(val), nil
	case types.NullKind:
		return nil, nil
	default:
		return nil, fmt.Errorf(`"%v" cannot convert NomsKind "%v" to a value`, ti.String(), k)
	}
}

// Equals implements TypeInfo interface.
func (ti *geometryType) Equals(other TypeInfo) bool {
	if other == nil {
		return false
	}
	if o, ok := other.(*geometryType); ok {
		// if either ti or other has defined SRID, then check SRID value; otherwise,
		return (!ti.sqlGeometryType.DefinedSRID && !o.sqlGeometryType.DefinedSRID) || ti.sqlGeometryType.SRID == o.sqlGeometryType.SRID
	}
	return false
}

// IsValid implements TypeInfo interface.
func (ti *geometryType) IsValid(v types.Value) bool {
	if _, ok := v.(types.Null); ok || v == nil {
		return true
	}

	switch v.(type) {
	case types.Geometry,
		types.Point,
		types.LineString,
		types.Polygon,
		types.MultiPoint,
		types.MultiLineString,
		types.MultiPolygon:
		return true
	default:
		return false
	}
}

// NomsKind implements TypeInfo interface.
func (ti *geometryType) NomsKind() types.NomsKind {
	return types.GeometryKind
}

// Promote implements TypeInfo interface.
func (ti *geometryType) Promote() TypeInfo {
	return ti
}

// String implements TypeInfo interface.
func (ti *geometryType) String() string {
	return "Geometry"
}

// ToSqlType implements TypeInfo interface.
func (ti *geometryType) ToSqlType() sql.Type {
	return ti.sqlGeometryType
}

func CreateGeometryTypeFromSqlGeometryType(sqlGeometryType gmstypes.GeometryType) TypeInfo {
	return &geometryType{sqlGeometryType: sqlGeometryType}
}
