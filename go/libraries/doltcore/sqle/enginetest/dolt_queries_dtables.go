// Copyright 2026 Dolthub, Inc.
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

package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
)

var DoltStatusTableScripts = []queries.ScriptTest{
	{
		Name: "dolt_status detached head is read-only clean",
		SetUpScript: []string{
			"CALL DOLT_COMMIT('--allow-empty', '-m', 'empty commit');",
			"CALL DOLT_TAG('tag1');",
			"SET @head_hash = (SELECT HASHOF('main') LIMIT 1);",
			"SET @status_by_hash = CONCAT('SELECT * FROM `mydb/', @head_hash, '`.dolt_status;');",
			"PREPARE status_by_hash FROM @status_by_hash;",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT * FROM `mydb/tag1`.dolt_status;",
				Expected: []sql.Row{},
			},
			{
				Query:    "EXECUTE status_by_hash;",
				Expected: []sql.Row{},
			},
			{
				Query:       "SELECT * FROM `information_schema`.dolt_status;",
				ExpectedErr: sql.ErrTableNotFound,
			},
		},
	},
}
