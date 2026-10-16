#!/usr/bin/env sh
PROJECTS="xepak-api xepak-api-rs xepak-mcp xepak-mcp-rs xepak-rest xepak-rest-rs xepak-rs"

SELF_DIR=$(realpath `dirname $0`)
cd "$SELF_DIR"

for proj in $PROJECTS; do
    echo "Publishing $proj..."
    cd "$proj" || continue
    cargo publish || echo "Failed to publish $proj"
    # rm -rf ./target
    cd -
done
