#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   sudo ./ultimate_bench.sh <receiver_host> <receiver_user> <source_path> <runs>
#
# Example:
#   sudo ./ultimate_bench.sh 158.247.237.42 root /root/test_10GiB.bin 3

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <receiver_host> <receiver_user> <source_path> <runs>"
  exit 1
fi

RECEIVER_HOST="$1"
RECEIVER_USER="$2"
SRC="$3"
RUNS="$4"

REMOTE_BENCH_DIR="/root/bench"

if [ ! -e "$SRC" ]; then
  echo "Source not found: $SRC"
  exit 1
fi

if ! [[ "$RUNS" =~ ^[0-9]+$ ]] || [ "$RUNS" -le 0 ]; then
  echo "runs must be a positive integer"
  exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
  echo "Please run as root: sudo $0 ..."
  exit 1
fi

for cmd in ssh scp rsync croc wormhole thru grep sed awk sort mktemp date; do
  command -v "$cmd" >/dev/null 2>&1 || { echo "Missing dependency: $cmd"; exit 1; }
done

WORKDIR="./bench_logs/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$WORKDIR"

touch "$WORKDIR/scp.txt" \
      "$WORKDIR/rsync.txt" \
      "$WORKDIR/croc.txt" \
      "$WORKDIR/thru.txt" \
      "$WORKDIR/thru_turn.txt" \
      "$WORKDIR/wormhole.txt"

echo "Persistent logs/results directory: $WORKDIR"

now_ms() {
  date +%s%3N
}

remote_exec() {
  ssh -o StrictHostKeyChecking=no "${RECEIVER_USER}@${RECEIVER_HOST}" "$@"
}

record_result() {
  local tool="$1"
  local ms="$2"
  echo "$ms" >> "$WORKDIR/$tool.txt"
}

median_file() {
  local file="$1"
  sort -n "$file" | awk '
    { a[NR]=$1 }
    END {
      if (NR == 0) exit 1
      if (NR % 2 == 1) print a[(NR+1)/2]
      else print (a[NR/2] + a[NR/2+1]) / 2
    }
  '
}

print_report_for_tool() {
  local tool="$1"
  local file="$WORKDIR/$tool.txt"

  echo "=== $tool ==="
  if [ ! -s "$file" ]; then
    echo "  no data"
    echo
    return
  fi

  local i=1
  while IFS= read -r ms; do
    awk -v run="$i" -v ms="$ms" 'BEGIN { printf("  run %d: %.3f sec (%d ms)\n", run, ms/1000, ms) }'
    i=$((i+1))
  done < "$file"

  local med
  med="$(median_file "$file")"
  awk -v med="$med" 'BEGIN { printf("  median: %.3f sec (%.0f ms)\n\n", med/1000, med) }'
}

prepare_run() {
  local tool="$1"
  local run="$2"

  echo "  [prep] clearing sender cache..."
  sync
  echo 3 > /proc/sys/vm/drop_caches

  echo "  [prep] clearing receiver cache and bench dir..."
  remote_exec "
    sync &&
    echo 3 > /proc/sys/vm/drop_caches &&
    rm -rf '$REMOTE_BENCH_DIR' &&
    mkdir -p '$REMOTE_BENCH_DIR'
  " >/dev/null

  {
    echo "tool=$tool"
    echo "run=$run"
    echo "timestamp=$(date -Iseconds)"
    echo "source=$SRC"
    echo "receiver_host=$RECEIVER_HOST"
    echo "receiver_user=$RECEIVER_USER"
    echo "remote_bench_dir=$REMOTE_BENCH_DIR"
  } > "$WORKDIR/${tool}_run${run}_meta.txt"
}

run_scp_once() {
  local run="$1"
  prepare_run "scp" "$run"

  local start end elapsed
  local sender_log="$WORKDIR/scp_run${run}_sender.log"

  start="$(now_ms)"
  if [ -d "$SRC" ]; then
    scp -r "$SRC" "${RECEIVER_USER}@${RECEIVER_HOST}:${REMOTE_BENCH_DIR}/" >"$sender_log" 2>&1
  else
    scp "$SRC" "${RECEIVER_USER}@${RECEIVER_HOST}:${REMOTE_BENCH_DIR}/" >"$sender_log" 2>&1
  fi
  end="$(now_ms)"

  elapsed=$((end - start))
  echo "  -> scp: ${elapsed} ms"
  record_result "scp" "$elapsed"
}

run_rsync_once() {
  local run="$1"
  prepare_run "rsync" "$run"

  local start end elapsed
  local sender_log="$WORKDIR/rsync_run${run}_sender.log"

  start="$(now_ms)"
  rsync -a --info=progress2 --no-compress --whole-file --inplace \
    "$SRC" "${RECEIVER_USER}@${RECEIVER_HOST}:${REMOTE_BENCH_DIR}/" >"$sender_log" 2>&1
  end="$(now_ms)"

  elapsed=$((end - start))
  echo "  -> rsync: ${elapsed} ms"
  record_result "rsync" "$elapsed"
}

run_croc_once() {
  local run="$1"
  prepare_run "croc" "$run"

  local raw_log="$WORKDIR/croc_run${run}_sender_raw.log"
  local clean_log="$WORKDIR/croc_run${run}_sender_clean.log"
  local recv_log="$WORKDIR/croc_run${run}_receiver_ssh.log"
  local expect_script="$WORKDIR/croc_run${run}_sender.expect"
  local code start end elapsed recv_status

  : > "$raw_log"
  : > "$clean_log"
  : > "$recv_log"

  cat > "$expect_script" <<'EOF'
#!/usr/bin/expect -f
set timeout -1

set src [lindex $argv 0]
set raw_log [lindex $argv 1]

log_file -noappend $raw_log
spawn croc $src

expect {
    -re {Did you mean to send.*\(Y/n\)} {
        send -- "Y\r"
        exp_continue
    }
    eof
}
EOF
  chmod +x "$expect_script"

  start="$(now_ms)"

  "$expect_script" "$SRC" "$raw_log" >/dev/null 2>&1 &
  local sender_pid=$!

  code=""
  for _ in $(seq 1 300); do
    if [ -f "$raw_log" ]; then
      sed -E 's/\x1B\[[0-9;]*[A-Za-z]//g' "$raw_log" | tr -d '\r' > "$clean_log" || true

      if grep -q 'Code is:' "$clean_log"; then
        code="$(grep 'Code is:' "$clean_log" | tail -n1 | sed -E 's/.*Code is: *([^[:space:]]+).*/\1/')"
        break
      fi
    fi
    sleep 0.2
  done

  if [ -z "$code" ]; then
    echo "  !! croc: failed to detect code"
    echo "  ---- croc sender log ----"
    cat "$clean_log" || true
    echo "  -------------------------"
    kill "$sender_pid" 2>/dev/null || true
    wait "$sender_pid" 2>/dev/null || true
    return 1
  fi

  echo "  detected croc code: $code"

  remote_exec "cd '$REMOTE_BENCH_DIR' && CROC_SECRET='$code' croc --yes >/dev/null 2>&1" >"$recv_log" 2>&1 &
  local recv_pid=$!

  if wait "$recv_pid"; then
    recv_status=0
  else
    recv_status=$?
  fi

  end="$(now_ms)"
  elapsed=$((end - start))

  if [ "$recv_status" -ne 0 ]; then
    echo "  !! croc receiver exited with status $recv_status"
    echo "  ---- croc receiver ssh log ----"
    cat "$recv_log" || true
    echo "  --------------------------------"
    kill "$sender_pid" 2>/dev/null || true
    wait "$sender_pid" 2>/dev/null || true
    return 1
  fi

  echo "  -> croc: ${elapsed} ms"
  record_result "croc" "$elapsed"

  wait "$sender_pid" 2>/dev/null || true
}

run_thru_once() {
  local run="$1"
  prepare_run "thru" "$run"

  local raw_log="$WORKDIR/thru_run${run}_sender_raw.log"
  local clean_log="$WORKDIR/thru_run${run}_sender_clean.log"
  local recv_log="$WORKDIR/thru_run${run}_receiver_ssh.log"
  local code start end elapsed

  start="$(now_ms)"

  thru host "$SRC" >"$raw_log" 2>&1 &
  local sender_pid=$!

  code=""
  for _ in $(seq 1 300); do
    sed -E 's/\x1B\[[0-9;]*[A-Za-z]//g' "$raw_log" | tr -d '\r' > "$clean_log" || true

    if grep -q 'Run on receiver' "$clean_log"; then
      code="$(grep 'Run on receiver' "$clean_log" | tail -n1 | sed -E 's/.*thru join ([A-Z0-9-]+).*/\1/')"
      break
    fi

    sleep 0.2
  done

  if [ -z "$code" ]; then
    echo "  !! thru: failed to detect code"
    kill "$sender_pid" 2>/dev/null || true
    wait "$sender_pid" 2>/dev/null || true
    return 1
  fi

  echo "  detected thru code: $code"

  remote_exec "cd '$REMOTE_BENCH_DIR' && thru join '$code' --overwrite >/dev/null 2>&1" >"$recv_log" 2>&1 &
  local recv_pid=$!

  wait "$recv_pid"
  end="$(now_ms)"
  elapsed=$((end - start))

  echo "  -> thru: ${elapsed} ms"
  record_result "thru" "$elapsed"

  kill -INT "$sender_pid" 2>/dev/null || true
  sleep 1
  kill -TERM "$sender_pid" 2>/dev/null || true
  wait "$sender_pid" 2>/dev/null || true
}

run_thru_turn_once() {
  local run="$1"
  prepare_run "thru_turn" "$run"

  local raw_log="$WORKDIR/thru_turn_run${run}_sender_raw.log"
  local clean_log="$WORKDIR/thru_turn_run${run}_sender_clean.log"
  local recv_log="$WORKDIR/thru_turn_run${run}_receiver_ssh.log"

  local code start end elapsed

  : > "$raw_log"
  : > "$clean_log"
  : > "$recv_log"

  start="$(now_ms)"

  thru host "$SRC" --force-turn >"$raw_log" 2>&1 &
  local sender_pid=$!

  code=""

  for _ in $(seq 1 300); do
    sed -E 's/\x1B\[[0-9;]*[A-Za-z]//g' "$raw_log" | tr -d '\r' > "$clean_log" || true

    if grep -q 'Run on receiver' "$clean_log"; then
      code="$(grep 'Run on receiver' "$clean_log" | tail -n1 | sed -E 's/.*thru join ([A-Z0-9-]+).*/\1/')"
      break
    fi

    sleep 0.2
  done

  if [ -z "$code" ]; then
    echo "  !! thru_turn: failed to detect code"
    echo "  ---- thru_turn sender log ----"
    cat "$clean_log"
    echo "  ------------------------------"
    kill "$sender_pid" 2>/dev/null || true
    wait "$sender_pid" 2>/dev/null || true
    return 1
  fi

  echo "  detected thru_turn code: $code"

  remote_exec "cd '$REMOTE_BENCH_DIR' && thru join '$code' --overwrite --force-turn >/dev/null 2>&1" >"$recv_log" 2>&1 &
  local recv_pid=$!

  wait "$recv_pid"

  end="$(now_ms)"
  elapsed=$((end - start))

  echo "  -> thru_turn: ${elapsed} ms"
  record_result "thru_turn" "$elapsed"

  kill -INT "$sender_pid" 2>/dev/null || true
  sleep 1
  kill -TERM "$sender_pid" 2>/dev/null || true
  wait "$sender_pid" 2>/dev/null || true
}

run_wormhole_once() {
  local run="$1"
  prepare_run "wormhole" "$run"

  local raw_log="$WORKDIR/wormhole_run${run}_sender_raw.log"
  local clean_log="$WORKDIR/wormhole_run${run}_sender_clean.log"
  local recv_log="$WORKDIR/wormhole_run${run}_receiver_ssh.log"
  local code start end elapsed

  start="$(now_ms)"

  wormhole send "$SRC" >"$raw_log" 2>&1 &
  local sender_pid=$!

  code=""
  for _ in $(seq 1 10000000); do
    sed -E 's/\x1B\[[0-9;]*[A-Za-z]//g' "$raw_log" | tr -d '\r' > "$clean_log" || true

    if grep -q 'Wormhole code is:' "$clean_log"; then
      code="$(grep 'Wormhole code is:' "$clean_log" | tail -n1 | sed -E 's/.*Wormhole code is: *([^[:space:]]+).*/\1/')"
      break
    fi

    sleep 0.2
  done

  if [ -z "$code" ]; then
    echo "  !! wormhole: failed to detect code"
    kill "$sender_pid" 2>/dev/null || true
    wait "$sender_pid" 2>/dev/null || true
    return 1
  fi

  echo "  detected wormhole code: $code"

  remote_exec "cd '$REMOTE_BENCH_DIR' && wormhole receive '$code' --accept-file >/dev/null 2>&1" >"$recv_log" 2>&1 &
  local recv_pid=$!

  wait "$recv_pid"
  end="$(now_ms)"
  elapsed=$((end - start))

  echo "  -> wormhole: ${elapsed} ms"
  record_result "wormhole" "$elapsed"

  wait "$sender_pid" 2>/dev/null || true
}

run_tool_n_times() {
  local tool="$1"
  local func="$2"

  echo
  echo "##### $tool #####"
  for i in $(seq 1 "$RUNS"); do
    echo "[$tool run $i/$RUNS]"
    "$func" "$i"
  done
}

run_tool_n_times "croc"     run_croc_once
run_tool_n_times "thru"     run_thru_once
run_tool_n_times "thru_turn" run_thru_turn_once
run_tool_n_times "wormhole" run_wormhole_once
run_tool_n_times "scp"      run_scp_once
run_tool_n_times "rsync"    run_rsync_once

echo
echo "================ FINAL REPORT ================"
print_report_for_tool "scp"
print_report_for_tool "rsync"
print_report_for_tool "croc"
print_report_for_tool "thru"
print_report_for_tool "wormhole"
print_report_for_tool "thru_turn"
