#!/usr/bin/env bash
# Starts a throwaway standalone ZooKeeper server for local development and
# integration tests, prints `export` lines, and stops it when given `stop`.
#
#   eval "$(scripts/dev-zk.sh start)"   # sets ZK_CONNECT, ZK_DEV_DIR
#   scripts/dev-zk.sh stop
set -eu
cmd=${1:-start}
state=${ZK_DEV_STATE:-${TMPDIR:-/tmp}/zk-dev-state}

find_home() {
  if [ -n "${ZOOKEEPER_HOME:-}" ] && [ -d "$ZOOKEEPER_HOME/lib" ]; then
    echo "$ZOOKEEPER_HOME"; return
  fi
  ls -d "$HOME"/.cache/zk/apache-zookeeper-*-bin 2>/dev/null | sort | tail -1
}

case "$cmd" in
  start)
    home=$(find_home)
    [ -n "$home" ] || { echo "no ZooKeeper distribution; set ZOOKEEPER_HOME" >&2; exit 1; }
    dir=$(mktemp -d "${TMPDIR:-/tmp}/zk-dev.XXXXXX")
    port=$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1])')
    mkdir -p "$dir/data"
    cat > "$dir/zoo.cfg" <<EOF
tickTime=500
dataDir=$dir/data
clientPort=$port
clientPortAddress=127.0.0.1
admin.enableServer=false
4lw.commands.whitelist=*
extendedTypesEnabled=true
minSessionTimeout=1000
EOF
    nohup java -Xmx256m -Dzookeeper.log.dir="$dir" -cp "$home/lib/*" \
      org.apache.zookeeper.server.quorum.QuorumPeerMain "$dir/zoo.cfg" > "$dir/zk.out" 2>&1 &
    echo "$! $dir" > "$state"
    for _ in $(seq 1 100); do
      if [ "$(printf ruok | nc 127.0.0.1 "$port" 2>/dev/null)" = imok ]; then break; fi
      sleep 0.2
    done
    echo "export ZK_CONNECT=127.0.0.1:$port ZK_DEV_DIR=$dir"
    ;;
  stop)
    [ -f "$state" ] || exit 0
    read -r pid dir < "$state"
    kill "$pid" 2>/dev/null || true
    rm -f "$state"
    case "$dir" in "${TMPDIR:-/tmp}"/zk-dev.*) rm -rf "$dir" ;; esac
    ;;
  *) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
