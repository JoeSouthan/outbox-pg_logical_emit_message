# Producer image: Go load generator that writes business rows and emits outbox events
# inside transactions, through PgBouncer, with a configurable worker pool.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/producer .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/producer /producer
ENTRYPOINT ["/producer"]
