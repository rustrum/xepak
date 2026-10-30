

test-one test_name:
    cargo test {{test_name}} -- --no-capture

clippy:
    cargo clippy --release --workspace --locked --tests --all-features --no-deps -- -D warnings

fmt:
    cargo fmt --check

audit:
    cargo audit -D warnings

# Clean temporary agents artifacts
agents-clean:
    rm -rf ./agents_work/

