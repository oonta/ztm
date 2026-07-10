FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ztm-node ./cmd/ztm-node

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

COPY --from=build /out/ztm-node /usr/local/bin/ztm-node
COPY deploy/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 7443 7444 7946 8080 9090

ENTRYPOINT ["/entrypoint.sh"]
