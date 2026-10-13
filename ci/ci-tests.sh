#!/bin/bash
set -eo pipefail

# Runs the test suite against a throwaway PostgreSQL instance. The database cluster lives in a
# temporary directory, only listens on a Unix socket within that directory, and is deleted once
# the tests complete.
#
# The PostgreSQL server binaries (initdb, pg_ctl and postgres) are taken from $POSTGRES_BIN_DIR if
# set, otherwise from $PATH, falling back to Debian's /usr/lib/postgresql/<version>/bin.
#
# Any arguments are passed through to `cargo test`.

if [ -n "$POSTGRES_BIN_DIR" ]; then
	PATH="$POSTGRES_BIN_DIR:$PATH"
elif ! command -v initdb > /dev/null; then
	for DIR in /usr/lib/postgresql/*/bin; do
		[ -x "$DIR/initdb" ] && POSTGRES_BIN_DIR="$DIR"
	done
	[ -n "$POSTGRES_BIN_DIR" ] && PATH="$POSTGRES_BIN_DIR:$PATH"
fi
for BIN in initdb pg_ctl postgres; do
	if ! command -v "$BIN" > /dev/null; then
		echo "Could not find $BIN, set POSTGRES_BIN_DIR to the directory containing the PostgreSQL server binaries" >&2
		exit 1
	fi
done

PG_DIR="$(mktemp -d)"
cleanup() {
	pg_ctl stop -D "$PG_DIR/data" -m immediate > /dev/null 2>&1 || true
	rm -rf -- "$PG_DIR"
}
trap cleanup EXIT

# Only our user can reach the socket in the (0700) temp directory, so trust auth is fine.
initdb -D "$PG_DIR/data" -U postgres --auth=trust --encoding=UTF8 --locale=C --no-sync > /dev/null
cat >> "$PG_DIR/data/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$PG_DIR'
fsync = off
EOF
if ! pg_ctl start -D "$PG_DIR/data" -l "$PG_DIR/postgres.log" -w > /dev/null; then
	cat "$PG_DIR/postgres.log" >&2
	exit 1
fi

export RAPID_GOSSIP_TEST_DB_HOST="$PG_DIR"
export RAPID_GOSSIP_TEST_DB_USER=postgres
export RAPID_GOSSIP_TEST_DB_NAME=postgres
cargo test "$@"
