# old_dolt runs $DOLT_OLD_BIN when set, otherwise the dolt on PATH.
old_dolt() {
  if [ -n "$DOLT_OLD_BIN" ]; then
    "$DOLT_OLD_BIN" "$@"
  else
    dolt "$@"
  fi
}

# new_dolt runs $DOLT_NEW_BIN when set, otherwise the dolt on PATH.
new_dolt() {
  if [ -n "$DOLT_NEW_BIN" ]; then
    "$DOLT_NEW_BIN" "$@"
  else
    dolt "$@"
  fi
}

if [ -z "$BATS_TMPDIR" ]; then
    export BATS_TMPDIR=$HOME/batstmp/
    mkdir "$BATS_TMPDIR"
fi

setup_common() {
    echo "setup" > /dev/null
}

teardown_common() {
    echo "teardown" > /dev/null
}

dolt config --global --add metrics.disabled true > /dev/null 2>&1
