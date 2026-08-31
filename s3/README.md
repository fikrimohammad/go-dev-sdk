# s3

A lightweight wrapper around the AWS SDK for Go v2 S3 transfer manager, S3 client, and presign
client with standard AWS SDK types and automatic OpenTelemetry tracing + metrics per operation via Smithy middleware.

## Features

- **Standard AWS SDK Types** — operates directly on standard AWS SDK types (`*s3.PutObjectInput`, `*s3.GetObjectInput`, `*transfermanager.UploadObjectInput`, etc.) with zero custom struct wrappers or mapping layers.
- **Standard AWS SDK Methods** — standard S3 operations (`PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, `DeleteObjects`, `CopyObject`, `ListObjectsV2`, `CreateBucket`, `DeleteBucket`, `HeadBucket`, `ListBuckets`) are natively available on `Client`.
- **IsNotFound** — ergonomic helper for checking 404 / NoSuchKey / NoSuchBucket errors reliably.
- **Smithy Middleware Telemetry** — automatic OpenTelemetry spans and `s3.client.operation.{count,duration}` metrics for all operations without manual method overrides.
- **Injectable Telemetry** — optionally override package-level metrics/tracer defaults via `WithMetrics` and `WithTracer`.

## Installation

```bash
go get github.com/fikrimohammad/go-dev-sdk/s3
```

## Step-by-step

### 1. Configure

```go
cfg := s3.Config{
    Region: "ap-southeast-1",

    // Optional: static credentials (including STS session tokens).
    // When empty, the default AWS credential chain (env, shared config, EC2/ECS roles) is used.
    AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
    SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
    SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),

    // Optional: self-hosted S3 (MinIO, Ceph, Cloudflare R2, LocalStack).
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

### 3. Upload an object (Transfer Manager Multipart Upload)

```go
file, err := os.Open("report.pdf")
if err != nil { /* handle */ }
defer file.Close()

out, err := cli.UploadObject(ctx, &transfermanager.UploadObjectInput{
    Bucket:             aws.String("reports"),
    Key:                aws.String("2026/08/report.pdf"),
    Body:               file, // any io.Reader
    ContentType:        aws.String("application/pdf"),
    ContentDisposition: aws.String("attachment; filename=report.pdf"),
    Metadata:           map[string]string{"uploaded-by": "user-123"},
})
if err != nil { /* handle */ }
fmt.Printf("Uploaded object ETag: %s\n", *out.ETag)
```

### 4. Get object as a stream (`GetObject`)

```go
res, err := cli.GetObject(ctx, &transfermanager.GetObjectInput{
    Bucket: aws.String("reports"),
    Key:    aws.String("2026/08/report.pdf"),
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
_, err = cli.DownloadObject(ctx, &transfermanager.DownloadObjectInput{
    Bucket:   aws.String("reports"),
    Key:      aws.String("2026/08/report.pdf"),
    WriterAt: outFile,
})
if err != nil { /* handle */ }
```

### 6. Copy object server-side (`CopyObject`)

```go
out, err := cli.CopyObject(ctx, &s3.CopyObjectInput{
    Bucket:     aws.String("archive"),
    Key:        aws.String("2026/08/report.pdf"),
    CopySource: aws.String("reports/2026/08/report.pdf"),
})
if err != nil { /* handle */ }
fmt.Printf("Copied object ETag: %s\n", *out.CopyObjectResult.ETag)
```

### 7. Presign download & upload URLs

```go
// Presigned download URL
req, err := cli.PresignGetObject(ctx, &s3.GetObjectInput{
    Bucket: aws.String("reports"),
    Key:    aws.String("2026/08/report.pdf"),
}, func(o *s3.PresignOptions) {
    o.Expires = 15 * time.Minute
})
if err != nil { /* handle */ }
fmt.Println("Download URL:", req.URL)

// Presigned upload URL (for direct frontend uploads)
req, err = cli.PresignPutObject(ctx, &s3.PutObjectInput{
    Bucket:      aws.String("uploads"),
    Key:         aws.String("user-avatar.png"),
    ContentType: aws.String("image/png"),
}, func(o *s3.PresignOptions) {
    o.Expires = 10 * time.Minute
})
if err != nil { /* handle */ }
fmt.Println("Upload URL:", req.URL)
```

### 8. Metadata inspection & 404 Handling

```go
// HeadObject inspection
info, err := cli.HeadObject(ctx, &s3.HeadObjectInput{
    Bucket: aws.String("reports"),
    Key:    aws.String("2026/08/report.pdf"),
})
if err == nil {
    fmt.Printf("Size: %d bytes, ETag: %s\n", *info.ContentLength, *info.ETag)
} else if s3.IsNotFound(err) {
    fmt.Println("Object not found!")
}
```

## Config reference

| Field | Default | Notes |
| --- | --- | --- |
| `Region` | — | Required |
| `Endpoint` | — | Optional; must be `http`/`https` |
| `AccessKeyID` / `SecretAccessKey` | — | Both set or both empty |
| `SessionToken` | — | Optional STS session token for temporary credentials |
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
| `Client` | Unified S3 client interface providing standard AWS SDK methods, Transfer Manager methods, Presign methods, and helpers |
| `IsNotFound(err)` | Helper returns `true` for 404 / `NoSuchKey` / `NoSuchBucket` errors |
| `DefaultPresignExpiry` | Package default URL validity (15m) |
