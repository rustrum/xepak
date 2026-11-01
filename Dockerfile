FROM rust:1.99-alpine AS builder

RUN apk update && apk add --no-cache musl-dev

WORKDIR /build

COPY . .

RUN cargo build --release


FROM alpine:latest AS runner

COPY --from=builder /build/target/release/xepak /bin/

EXPOSE 8080

ENV RUST_LOG=info,xepak=info

ENTRYPOINT ["/bin/xepak", "-p", "8080"]
