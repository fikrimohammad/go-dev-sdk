# s3

A thin wrapper around the AWS SDK for Go v2 S3 transfer manager, S3 client, and presign
client with a standardized API and automatic OpenTelemetry tracing + metrics per operation.

## Features

- **UploadObject** — uploads through the transfer manager, which transparently
  performs **multipart uploads** for large bodies (accepts any `io.Reader`, with
  support for custom metadata, storage classes, cache control, and content disposition).
- **GetObject** — retrieves objects as streams (`io.ReadCloser`) with concurrent
  chunk fetching and bounded buffer memory.
- **DownloadObject** — downloads directly into an `io.WriterAt` (e.g. `*os.File`),
  writing concurrent multipart chunks directly to disk in parallel.
- **CopyObject** — server-side copying of objects with zero egress bandwidth consumption.
- **ObjectExists & IsNotFound** — ergonomic helpers for checking existence and 404 errors.
- **DeleteObject & DeleteObjects** — single-key and automatic batch chunking object deletion.
- **HeadObject** — fast metadata and existence lookup without downloading content.
- **ListObjects** — prefix-based object and folder listing with continuation token pagination.
- **PresignGetObject** — returns a presigned download URL with response header overrides.
- **PresignPutObject** — returns a presigned upload URL enabling direct browser/mobile to S3 uploads.
- **Self-hosted S3 & Path-Style** — set an `Endpoint` (MinIO, Ceph, LocalStack, Cloudflare R2) with configurable `UsePathStyle`.
- **Telemetry** — one client span per operation plus
  `s3.client.operation.{count,duration}` metrics with standard OTel attributes
  (`rpc.system`, `rpc.service`, `rpc.method`, `aws.s3.bucket`,
  `cloud.region`, and `server.*` for self-hosted endpoints).
- **Error classification** — `error.type` maps AWS API error codes, transport
  failures (`timeout`, `connection_reset`, `dns_error`, `canceled`, ...), or the raw message.
- **Injectable telemetry** — `WithMetrics` / `WithTracer` override package-level defaults.

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

    // Optional: self-hosted S3 (MinIO, Ceph, Cloudflare R2, ...).
    Endpoint:     "http://localhost:9000",
    // UsePathStyle: &[]bool{true}[0], // optional override

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
    Bucket:             "reports",
    Key:                "2026/08/report.pdf",
    Body:               file, // any io.Reader
    ContentType:        "application/pdf",
    ContentDisposition: "attachment; filename=report.pdf",
    Metadata:           map[string]string{"uploaded-by": "user-123"},
})
if err != nil { /* handle */ }
```

### 4. Get object as a stream (`GetObject`)

```go
res, err := cli.GetObject(ctx, s3.GetObjectParams{
    Bucket: "reports",
    Key:    "2026/08/report.pdf",
})
if err != nil { /* handle */ }
defer res.Body.Close()

data, err := io.ReadAll(res.Body)
```

### 5. Download object directly to file (`DownloadObject`)

```go
outFile, err := os.Create("downloaded-report.pdf")
if err != nil { /* handle */ }
defer outFile.Close()

// Concurrent multipart download directly into the file via io.WriterAt
res, err := cli.DownloadObject(ctx, s3.DownloadObjectParams{
    Bucket: "reports",
    Key:    "2026/08/report.pdf",
    Writer: outFile,
})
if err != nil { /* handle */ }
fmt.Printf("Downloaded %d bytes, ETag: %s\n", res.ContentLength, res.ETag)
```

### 6. Copy object server-side (`CopyObject`)

```go
res, err := cli.CopyObject(ctx, s3.CopyObjectParams{
    SourceBucket: "reports",
    SourceKey:    "2026/08/report.pdf",
    DestBucket:   "archive",
    DestKey:      "2026/08/report.pdf",
    StorageClass: "STANDARD_IA",
})
if err != nil { /* handle */ }
fmt.Printf("Copied object ETag: %s\n", res.ETag)
```

### 7. Presign download & upload URLs

```go
// Presigned download URL
downloadURL, err := cli.PresignGetObject(ctx, s3.PresignGetObjectParams{
    Bucket:    "reports",
    Key:       "2026/08/report.pdf",
    ExpiresIn: 15 * time.Minute,
})

// Presigned upload URL (for direct frontend uploads)
uploadURL, err := cli.PresignPutObject(ctx, s3.PresignPutObjectParams{
    Bucket:      "uploads",
    Key:         "user-avatar.png",
    ContentType: "image/png",
    ExpiresIn:   10 * time.Minute,
})
```

### 8. Delete objects

```go
// Single deletion
err = cli.DeleteObject(ctx, s3.DeleteObjectParams{
    Bucket: "reports",
    Key:    "old-report.pdf",
})

// Batch deletion (automatically chunked into 1,000 keys per batch)
delRes, err := cli.DeleteObjects(ctx, s3.DeleteObjectsParams{
    Bucket: "reports",
    Keys:   []string{"temp1.csv", "temp2.csv"},
})
```

### 9. Metadata inspection & Existence checking

```go
// Fast existence check
exists, err := cli.ObjectExists(ctx, "reports", "2026/08/report.pdf")
if err != nil { /* handle */ }
if !exists {
    fmt.Println("Object does not exist")
}

// Check metadata
info, err := cli.HeadObject(ctx, s3.HeadObjectParams{
    Bucket: "reports",
    Key:    "2026/08/report.pdf",
})
if err == nil {
    fmt.Printf("Size: %d bytes, ETag: %s\n", info.ContentLength, info.ETag)
} else if s3.IsNotFound(err) {
    fmt.Println("Not found!")
}

// List objects matching prefix
list, err := cli.ListObjects(ctx, s3.ListObjectsParams{
    Bucket:  "reports",
    Prefix:  "2026/",
    MaxKeys: 100,
})
for _, obj := range list.Objects {
    fmt.Println(obj.Key, obj.Size)
}
```

## Config reference

| Field | Default | Notes |
| --- | --- | --- |
| `Region` | — | Required |
| `Endpoint` | — | Optional; must be `http`/`https` |
| `AccessKeyID` / `SecretAccessKey` | — | Both set or both empty |
| `UsePathStyle` | `true` if `Endpoint` is set | Forces path-style addressing |
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
| `Client` | Full S3 operations interface |
| `IsNotFound(err)` | Helper returns `true` for 404 / `NoSuchKey` / `NoSuchBucket` errors |
| `UploadObjectParams` | Upload options with `io.Reader`, `Metadata`, `StorageClass`, headers |
| `GetObjectParams` / `GetObjectResult` | Stream download with content headers and metadata |
| `DownloadObjectParams` / `DownloadObjectResult` | Parallel multipart download directly into an `io.WriterAt` (e.g. `*os.File`) |
| `CopyObjectParams` / `CopyObjectResult` | Server-side copying of objects |
| `DeleteObjectParams` / `DeleteObjectsParams` | Single and chunked batch deletion |
| `HeadObjectParams` / `ObjectInfo` | Metadata inspection |
| `ListObjectsParams` / `ListObjectsResult` | Prefix-based directory listing |
| `PresignGetObjectParams` / `PresignPutObjectParams` | Presigned download & upload URLs |
| `DefaultPresignExpiry` | Package default URL validity (15m) |
