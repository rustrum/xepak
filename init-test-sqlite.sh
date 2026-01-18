#!/usr/bin/env sh

current_dir="$(dirname "$0")"

db_file="$current_dir/db.sqlite3"

sql_file="$current_dir/examples/db-sqlite.sql"

if [ -f "$db_file" ]; then
    echo "Nothing to do! DB file already exists $db_file"
    exit 0
fi

echo "Creating DB $db_file"

sqlite3 "$db_file" < "$sql_file"

echo "Done"
