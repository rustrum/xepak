
# Clean temporary agents artifacts
agents-clean:
    rm -rf ./agents_work/

clippy:
    cargo clippy --release --workspace --locked --tests --all-features --no-deps -- -D warnings

fmt:
    cargo fmt --check

audit:
    cargo audit -D warnings
