# s3

A thin wrapper around the AWS SDK for Go v2 S3 transfer manager and presign
client with a small, standardized API and automatic OpenTelemetry tracing +
metrics per operation.

## Features

- **UploadObject** — uploads through the transfer manager, which transparently
  performs **multipart uploads** for large bodies (with configurable part size,
  threshold, and concurrency).
- **PresignGetObject** — returns a presigned download URL with response
  content-type / content-disposition overrides and per-call expiry.
- **Self-hosted S3 support** — set an `Endpoint` (e.g. MinIO); path-style
  addressing is enabled automatically and the endpoint is surfaced in
  telemetry.
- **Telemetry** — one client span per operation plus
  `s3.client.operation.{count,duration}` metrics with OTel attributes
  (`rpc.system`, `rpc.service`, `rpc.method`, `aws.s3.bucket`,
  `cloud.region`, and `server.*` for self-hosted endpoints).
- **Error classification** — `error.type` maps AWS API error codes, transport
  failures (`timeout`, `connection_reset`, `dns_error`, ...), or the raw
  message.
- **Injectable telemetry** — `WithMetrics` / `WithTracer` override the
  package-level observability defaults.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/s3
```

## Step-by-step

### 1. Configure

```go
cfg := s3.Config{
    Region: "ap-southeast-1",

    // Optional: static credentials. When empty, the default AWS credential
    // chain (env, shared config, EC2/ECS roles) is used.
    AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
    SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),

    // Optional: self-hosted S3 (MinIO, Ceph, ...). Path-style + telemetry
    // server attrs are derived from it.
    Endpoint: "http://localhost:9000",

    // Optional multipart tuning (zero = transfer manager defaults).
    // UploadPartSizeBytes: 8 << 20, // min 5MB
    // UploadMultipartThreshold: 16 << 20,
    // TransferConcurrency: 5,
}
```

### 2. Connect

```go
cli, err := s3.New(cfg)
if err != nil { /* handle */ }
```

### 3. Upload an object

```go
file, err := os.Open("report.pdf")
if err != nil { /* handle */ }
defer file.Close()

err = cli.UploadObject(ctx, s3.UploadObjectParams{
    Bucket:      "reports",
    Key:         "2026/08/report.pdf",
    Body:        file, // io.ReadCloser
    ContentType: "application/pdf",
})
if err != nil { /* handle */ }
```

Large bodies are uploaded in parts automatically; `UploadPartSizeBytes`,
`UploadMultipartThreshold`, and `TransferConcurrency` tune the transfer
manager.

### 4. Presign a download URL

```go
url, err := cli.PresignGetObject(ctx, s3.PresignGetObjectParams{
    Bucket:              "reports",
    Key:                 "2026/08/report.pdf",
    ResponseContentType: "application/pdf", // overrides the stored content type
    ExpiresIn:           5 * time.Minute,   // zero → cfg.PresignDefaultExpiry (15m)
})
if err != nil { /* handle */ }
```

Return the URL to the client; it can download the object until it expires.

### 5. (Optional) Inject telemetry clients

```go
cli, err := s3.New(cfg, s3.WithMetrics(mc), s3.WithTracer(tc))
```

## Telemetry attributes

Per operation: `rpc.system` (`aws-api`), `rpc.service` (`s3`), `rpc.method`
(`UploadObject` / `PresignGetObject`), `aws.s3.bucket`, `cloud.region` (when
known), `server.address` + `server.port` (only for self-hosted endpoints), and
`error.type` (AWS API error code, a transport label like `timeout` /
`connection_reset` / `dns_error`, or the raw message; empty on success).

## Config reference

| Field | Default | Notes |
| --- | --- | --- |
| `Region` | — | Required |
| `Endpoint` | — | Optional; must be `http`/`https` |
| `AccessKeyID` / `SecretAccessKey` | — | Both set or both empty |
| `UploadPartSizeBytes` | transfer manager default (8MB) | Min 5MB |
| `UploadMultipartThreshold` | transfer manager default (16MB) | |
| `TransferConcurrency` | transfer manager default (5) | |
| `PresignDefaultExpiry` | `15m` | Used when a per-call expiry is not supplied |

## API reference

| Symbol | Description |
| --- | --- |
| `Config` | Connection + transfer settings; `SetDefaults()`, `Validate()` |
| `New(cfg, opts...)` | Build an instrumented `Client` |
| `WithMetrics` / `WithTracer` | Telemetry injection options |
| `UploadObjectParams` | `Bucket`, `Key`, `Body` (`io.ReadCloser`), `ContentType` |
| `PresignGetObjectParams` | `Bucket`, `Key`, `ResponseContentType`, `ResponseContentDisposition`, `ExpiresIn` |
| `Client` | `UploadObject`, `PresignGetObject` |
| `DefaultPresignExpiry` | Package default URL validity |
