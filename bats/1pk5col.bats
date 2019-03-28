#!/usr/bin/env bats

setup() {
    export PATH=$PATH:~/go/bin
    export NOMS_VERSION_NEXT=1
    cd $BATS_TMPDIR
    mkdir dolt-repo
    cd dolt-repo
    dolt init
    dolt table create -s=$BATS_TEST_DIRNAME/helper/1pk5col.schema test
}

teardown() {
    rm -rf $BATS_TMPDIR/dolt-repo
}

# Create a single primary key table and do stuff
@test "create a table with a schema file and examine repo" {
    run dolt ls
    [ "$status" -eq 0 ]
    [[ "${lines[1]}" =~ "test" ]]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "$output" = "pk|c1|c2|c3|c4|c5" ]
    run dolt diff
    [ "$status" -eq 0 ]
    [ "${lines[0]}" = "diff --dolt a/test b/test" ]
    [ "${lines[1]}" = "added table" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
}

@test "create a table, dolt add, dolt reset, and dolt commit" {
    run dolt add test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
    run dolt reset test 
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Untracked files" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
    run dolt add .
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt status
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Changes to be committed" ]]
    [[ "$output" =~ new[[:space:]]table:[[:space:]]+test ]]
    run dolt commit -m "test commit"
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test commit" ]]
}

@test "add a row to a created table using dolt table put-row" {
    dolt add test
    dolt commit -m "create table"
    run dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+0[[:space:]]+\|[[:space:]]+1 ]]
}

@test "delete a row with dolt table rm-row" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table rm-row test 0
    [ "$status" -eq 0 ]
    [ "$output" = "Removed 1 rows" ]
}

@test "delete multiple rows with dolt table rm-row" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt table put-row test pk:1 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table rm-row test 0 1
    [ "$status" -eq 0 ]
    [ "$output" = "Removed 2 rows" ]
}

@test "delete the table" {
    run dolt table rm test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls 
    [[ ! "$output" =~ "test" ]]
}

@test "move a table" {
    run dolt table mv test test1
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls
    [[ ! "$output" =~ "test" ]]
    [[ "$output" =~ "test1" ]]
}

@test "copy a table" {
    run dolt table cp test test1
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt ls
    [[ "$output" =~ "test" ]]
    [[ "$output" =~ "test1" ]]
}

@test "dolt checkout to put a table back to its checked in state" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "Added table and test row"
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:10
    run dolt checkout test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5" ]]
    [[ ! "$output" =~ "10" ]]
}

@test "dolt checkout branch and table name collision" {
    dolt branch test
    run dolt checkout test
    [ "$status" -eq 0 ]
    skip "behavior ambiguous right now. should reset test table and switch to branch per git"
}

@test "make a change on a different branch, commit, and merge to master" {
    dolt branch test-branch
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Fast-forward" ]]
    run dolt log
    [ "$status" -eq 0 ]
    [[ "$output" =~ "added test row" ]]
}

@test "generate a merge conflict and resolve with ours" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:6
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "CONFLICT (content)" ]]
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Cnf" ]]
    [[ "$output" =~ "!" ]]
    run dolt table select --hide-conflicts test
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "Cnf" ]]
    [[ ! "$output" =~ "!" ]]
    run dolt conflicts cat test
    [ "$status" -eq 0 ]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+ours[[:space:]]+\| ]]
    [[ "$output" =~ \+[[:space:]]+\|[[:space:]]+theirs[[:space:]]+\| ]]
    run dolt conflicts resolve --ours test
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Cnf" ]]
    [[ ! "$output" =~ "!" ]]
    [[ "$output" =~ "5" ]]
    [[ ! "$output" =~ "6" ]]
    dolt add test
    dolt commit -m "merged and resolved conflict"
    run dolt log
    [[ "$output" =~ "added test row" ]]
    [[ "$output" =~ "added conflicting row" ]]
    [[ "$output" =~ "merged and resolved conflict" ]]
    [[ "$output" =~ "Merge:" ]]
}

@test "generate a merge conflict and resolve with theirs" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:6
    dolt add test
    dolt commit -m "added conflicting test row"
    dolt checkout master
    dolt merge test-branch
    run dolt conflicts resolve --theirs test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]]
    [[ ! "$output" =~ "5" ]]
}

@test "put a row that violates the schema" {
    run dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:foo
    [ "$status" -ne 0 ]
    [ "${lines[0]}" = "inserted row does not match schema" ]
}

@test "import data from a csv file after table created" {
    run dolt table import test $BATS_TEST_DIRNAME/helper/1pk5col.csv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "import data from a psv file after table created" {
    run dolt table import test $BATS_TEST_DIRNAME/helper/1pk5col.psv
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "overwrite a row. make sure it updates not inserts" {
    dolt table import test $BATS_TEST_DIRNAME/helper/1pk5col.csv
    run dolt table put-row test pk:1 c1:2 c2:4 c3:6 c4:8 c5:10
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully put row." ]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "add row on two different branches. no merge conflict" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table put-row test pk:1 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row with one more column"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]]
    [[ ! "$output" =~ "CONFLICT" ]]
    run dolt table select test
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 3 ]
}

@test "add column, no merge conflict" {
    dolt add test
    dolt commit -m "added test table"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt checkout test-branch
    dolt table schema --add-field test c6 int 
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5 c6:6
    dolt add test
    dolt commit -m "added test row with one more column"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]]
    [[ ! "$output" =~ "CONFLICT" ]]
    run dolt diff 
    [ "$status" -eq 0 ]
    [[ "$output" =~ "6" ]]
}

@test "modify different fields, same row, no merge conflict" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "added test row"
    dolt branch test-branch
    dolt table put-row test pk:0 c1:2 c2:2 c3:3 c4:4 c5:5
    dolt add test
    dolt commit -m "modified c1 of test row"
    dolt checkout test-branch
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:10
    dolt add test
    dolt commit -m "modified c5 of test row"
    dolt checkout master
    run dolt merge test-branch
    [ "$status" -eq 0 ]
    [[ "$output" =~ "Updating" ]]
    [[ ! "$output" =~ "CONFLICT" ]]
    run dolt diff
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]]
}

@test "dolt table select with options" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    dolt table put-row test pk:1 c1:6 c2:7 c3:8 c4:9 c5:10
    dolt table put-row test pk:2 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table select --where pk=1 test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "10" ]]
    [[ ! "$output" =~ "5" ]]
    [ "${#lines[@]}" -eq 2 ]
    run dolt table select --where c1=1 test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "5" ]]
    [[ ! "$output" =~ "10" ]]
    [ "${#lines[@]}" -eq 3 ]
    run dolt table select test pk c1 c2 c3
    [ "$status" -eq 0 ]
    [[ ! "$output" =~ "c4" ]]
    run dolt table select --where c1=1 --limit=1 test
    skip "Adding --limit=1 panics right now" 
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
}

@test "dolt table export" {
    dolt table put-row test pk:0 c1:1 c2:2 c3:3 c4:4 c5:5
    run dolt table export test export.csv
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully exported data." ]
    [ -f export.csv ]
    run grep 5 export.csv
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 2 ]
    run dolt table export test export.csv
    [ "$status" -ne 0 ]
    [[ "$output" = "Data already exists in" ]]
    run dolt table export -f test export.csv
    [ "$status" -eq 0 ]
    [ "$output" = "Successfully exported data." ]
    [ -f export.csv ]
}

@test "dolt table schema" {
    run dolt table schema
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]]
    [[ "$output" =~ "columns" ]]
    [[ "$output" =~ "c1" ]]
    run dolt table schema test
    [ "$status" -eq 0 ]
    [[ "$output" =~ "test @ working" ]]
    [[ "$output" =~ "columns" ]]
    [[ "$output" =~ "c1" ]]
}

@test "dolt table schema on non existant table" {
    run dolt table schema foo
    [ "$status" -eq 0 ]
    [ "$output" = "foo not found" ]
}

@test "dolt table schema --export" {
    run dolt table schema --export test export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
    [ -f export.schema ]
    run diff $BATS_TEST_DIRNAME/helper/1pk5col.schema export.schema
    [ "$status" -eq 0 ]
    [ "$output" = "" ]
}

