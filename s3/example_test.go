// This file holds documentation examples for package s3. They are
// compile-only because they need a live S3-compatible server.

package s3_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/fikrimohammad/go-dev-sdk/s3"
)

// ExampleNew uploads a report when it is missing and prints a presigned
// download URL that expires after 15 minutes.
func ExampleNew() {
	client, err := s3.New(s3.Config{
		Region:          "us-east-1",
		Endpoint:        "http://localhost:9000",
		AccessKeyID:     "minioadmin",
		SecretAccessKey: "minioadmin",
	})
	if err != nil {
		log.Println(err)
		return
	}

	ctx := context.Background()
	const bucket, key = "reports", "2026/q4.txt"

	_, err = client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	switch {
	case s3.IsNotFound(err):
		_, err = client.PutObject(ctx, &awss3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader("quarterly totals"),
			ContentType: aws.String("text/plain"),
		})
		if err != nil {
			log.Println(err)
			return
		}
	case err != nil:
		log.Println(err)
		return
	}

	req, err := client.PresignGetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(o *awss3.PresignOptions) {
		o.Expires = 15 * time.Minute
	})
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Println(req.URL)
}
