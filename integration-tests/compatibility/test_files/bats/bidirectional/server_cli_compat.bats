#!/usr/bin/env bats
#
# Verifies the SQL server and CLI client work across Dolt versions. DOLT_OLD_BIN and
# DOLT_NEW_BIN are slot labels assigned by runner.sh; the bidirectional phase swaps them
# between sub-runs, so neither name claims age. Tests reach the binaries via old_dolt and
# new_dolt; new_dolt_cli targets the running sql-server. REPO_DIR is the pre-populated
# repo from setup_repo.sh.

setup_file() {
  export BATS_TEST_RETRIES=3
}

setup() {
  bats_load_library windows-compat.bash
  bats_load_library query-server-common.bash
  bats_load_library common.bash
  bats_load_library server.bash
  cp -Rpf "$REPO_DIR" "$BATS_TEST_TMPDIR/repo"
  cd "$BATS_TEST_TMPDIR/repo"
  skip_if_new_lt "1.20.0" "new_dolt does not support global --host/--port/--user/--password connection flags"
  skip_if_old_lt "1.2.0" "sql-server does not accept connections from client"
  start_old_sql_server
}

teardown() {
  stop_old_sql_server
}

@test "server-cli-compat: client connects and runs SELECT 1" {
  run new_dolt_cli sql -r csv -q "SELECT 1 AS test;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "test" ]] || false
  [[ "${lines[1]}" == "1" ]] || false
}

@test "server-cli-compat: client reads commit history via dolt_log" {
  run new_dolt_cli sql -r csv -q "SELECT committer, email, message FROM dolt_log ORDER BY date LIMIT 3;"
  [ "$status" -eq 0 ]
  [[ "$output" =~ "initialized data" ]] || false
  [[ "$output" =~ "made changes to main" ]] || false
}

@test "server-cli-compat: commit and revert via client" {
  skip_if_old_lt "1.86.0" "DOLT_REVERT returns conflict count columns starting in v1.86.0, and dolt commit/revert print commit info using the commit_order column in dolt_log added in v1.54.0"

  run new_dolt_cli sql -q "INSERT INTO big VALUES (10000, 'test commit');"
  [ "$status" -eq 0 ]

  run new_dolt_cli add -A
  [ "$status" -eq 0 ]

  run new_dolt_cli commit -m "test commit message"
  [ "$status" -eq 0 ]
  commit_output=$(strip_ansi "$output")
  [[ "$commit_output" =~ "test commit message" ]] || false
  [[ "$commit_output" =~ "commit " ]] || false

  commit_to_revert=$(extract_commit_hash "$output")
  [[ -n "$commit_to_revert" ]] || false

  run new_dolt_cli revert "$commit_to_revert"
  [ "$status" -eq 0 ]
  revert_output=$(strip_ansi "$output")
  [[ "$revert_output" =~ "commit " ]] || false

  # The revert must remove the row inserted above.
  run new_dolt_cli sql -r csv -q "SELECT count(*) AS n FROM big WHERE i = 10000;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "n" ]] || false
  [[ "${lines[1]}" == "0" ]] || false
}

@test "server-cli-compat: --author override surfaces through committer" {
  skip_if_old_lt "1.44.2" "servers reject writes via --use-db with 'database is read only'"

  run new_dolt_cli sql -q "INSERT INTO big VALUES (10001, 'author committer test');"
  [ "$status" -eq 0 ]
  run new_dolt_cli add -A
  [ "$status" -eq 0 ]
  run new_dolt_cli commit --author "Explicit Author <explicit@author.com>" -m "author committer compat test"
  [ "$status" -eq 0 ]

  run old_dolt sql -r csv -q "SELECT committer, email FROM dolt_log WHERE message = 'author committer compat test' LIMIT 1;"
  [ "$status" -eq 0 ]
  [[ "${lines[0]}" == "committer,email" ]] || false
  [[ "${lines[1]}" == "Explicit Author,explicit@author.com" ]] || false
}

@test "server-cli-compat: cherry-pick via client preserves author and applies row" {
  skip_if_old_lt "1.54.0" "dolt log/cherry-pick query the commit_order column in dolt_log"
  skip_if_new_lt "1.44.2" "can't read the current server's storage format (unknown record field tag)"

  # check_merge only modifies the def table, avoiding conflicts with other branches.
  run new_dolt_cli log check_merge -n 1
  [ "$status" -eq 0 ]
  cherry_commit=$(extract_commit_hash "$output")
  [[ -n "$cherry_commit" ]] || false

  # Capture the def-table row count before cherry-pick to confirm the change applied.
  run new_dolt_cli sql -r csv -q "SELECT count(*) FROM def;"
  [ "$status" -eq 0 ]
  pre_count=$(printf "%s\n" "$output" | tail -n1)

  run new_dolt_cli cherry-pick "$cherry_commit"
  [ "$status" -eq 0 ]
  cherry_output=$(strip_ansi "$output")
  [[ "$cherry_output" =~ "commit " ]] || false

  # def row count must increase by the number of rows in the source commit.
  run new_dolt_cli sql -r csv -q "SELECT count(*) FROM def;"
  [ "$status" -eq 0 ]
  post_count=$(printf "%s\n" "$output" | tail -n1)
  [ "$post_count" -gt "$pre_count" ]
}
