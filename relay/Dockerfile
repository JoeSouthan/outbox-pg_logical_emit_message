# Relay image: /relay (log-message relay, with optional journal recovery) and
# /tablerelay (table-outbox baseline). CMD so `docker compose run relay /tablerelay` works.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/relay /out/tablerelay /
CMD ["/relay"]
