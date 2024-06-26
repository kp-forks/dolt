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

package doltdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type isValidIndexNameTest struct {
	name      string
	indexName string
	valid     bool
}

var isValidIndexNameTests = []isValidIndexNameTest{
	{
		name:      "all valid ASCII characters",
		indexName: " ~!@#$%^&*()_+`-=[]{}|;':\",./<>?abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
		valid:     true,
	},
	{
		name:      "single char",
		indexName: "_",
		valid:     true,
	},
	{
		name:      "backticks in prefix/suffix",
		indexName: "``index``",
		valid:     true,
	},
	{
		name:      "backtick in middle",
		indexName: "in`dex",
		valid:     true,
	},
	{
		name:      "numeric",
		indexName: "1234",
		valid:     true,
	},
	{
		name:      "valid @ char",
		indexName: "as@df",
		valid:     true,
	},
	{
		name:      "valid [] chars",
		indexName: "[asdf]",
		valid:     true,
	},
	{
		name:      "spaces allowed",
		indexName: "    s p a c e s    ",
		valid:     true,
	},
	{
		name:      "empty string",
		indexName: "",
		valid:     false,
	},
}

func TestIsValidIndexName(t *testing.T) {
	for _, test := range isValidIndexNameTests {
		t.Run(test.name, func(t *testing.T) {
			valid := IsValidIdentifier(test.indexName)
			require.Equal(t, test.valid, valid)
		})
	}
}
