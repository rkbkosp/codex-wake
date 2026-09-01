FROM golang:1.24-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/codex-wait-relay ./cmd/codex-wait-relay
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/codex-wait-relay /usr/local/bin/codex-wait-relay
COPY --from=build --chown=65532:65532 /out/data /var/lib/codex-wait
ENV DATABASE_PATH=/var/lib/codex-wait/relay.db
ENV LISTEN_ADDR=0.0.0.0:8787
VOLUME ["/var/lib/codex-wait"]
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/codex-wait-relay"]
