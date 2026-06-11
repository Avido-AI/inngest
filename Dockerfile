FROM --platform=$BUILDPLATFORM golang:1.26-alpine@sha256:a6a091eac01ceac4b97496fe2957a49b6cdd83365337d5f46f6f73710424e805 AS build
RUN apk upgrade --no-cache && apk add --no-cache build-base
WORKDIR /app
COPY vendor vendor
COPY . .
ARG TARGETARCH
ARG TARGETOS
RUN --mount=type=cache,target=/root/.cache/go-build \
    GOFLAGS=-mod=vendor GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /go/bin/inngest ./cmd/

FROM alpine:3.24@sha256:a2d49ea686c2adfe3c992e47dc3b5e7fa6e6b5055609400dc2acaeb241c829f4 AS inngest
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates tzdata && update-ca-certificates
RUN addgroup -g 1000 -S inngest && adduser -u 1000 -S -G inngest -s /sbin/nologin inngest
COPY --from=build /go/bin/inngest /bin/inngest
USER inngest
CMD ["inngest"]
