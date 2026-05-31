# --- build -------------------------------------------------------------------
FROM golang:1.23 AS build
WORKDIR /src

# Cache deps first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Static binary (CGO off) so it runs on a distroless/scratch base. -s -w strips debug info.
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/dashboard .

# --- run ---------------------------------------------------------------------
# distroless/static: no shell, no package manager, runs as nonroot — tiny (~2 MB) and a nice
# contrast to the JVM images. The frontend is embedded in the binary, so nothing else to copy.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/dashboard /dashboard
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/dashboard"]
