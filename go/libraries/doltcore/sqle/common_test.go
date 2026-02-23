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

package sqle

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

// Runs the query given and returns the result. The schema result of the query's execution is currently ignored, and
// the targetSchema given is used to prepare all rows.
func executeSelect(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, query string) ([]sql.Row, sql.Schema, error) {
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)

	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)
	if err != nil {
		return nil, nil, err
	}

	sch, iter, _, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, nil, err
	}

	sqlRows := make([]sql.Row, 0)
	var r sql.Row
	for r, err = iter.Next(sqlCtx); err == nil; r, err = iter.Next(sqlCtx) {
		sqlRows = append(sqlRows, r)
	}

	if err != io.EOF {
		return nil, nil, err
	}

	return sqlRows, sch, nil
}

// Runs the query given and returns the error (if any).
func executeModify(t *testing.T, ctx context.Context, dEnv *env.DoltEnv, query string) (doltdb.RootValue, error) {
	db, err := NewDatabase(ctx, "dolt", dEnv.DbData(ctx), editor.Options{})
	require.NoError(t, err)

	engine, sqlCtx, err := NewTestEngine(dEnv, ctx, db)

	if err != nil {
		return nil, err
	}

	_, iter, _, err := engine.Query(sqlCtx, query)
	if err != nil {
		return nil, err
	}

	for {
		_, err := iter.Next(sqlCtx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}

	err = iter.Close(sqlCtx)
	if err != nil {
		return nil, err
	}

	return db.GetRoot(sqlCtx)
}
