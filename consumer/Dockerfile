# Consumer image: subscribes to a target's topic and prints what arrives.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/consumer .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/consumer /consumer
ENTRYPOINT ["/consumer"]
