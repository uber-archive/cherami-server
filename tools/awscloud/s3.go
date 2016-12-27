// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package awscloud

import (
	"crypto/rand"
	"encoding/hex"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type (
	// SecureS3Client is an interface that all
	// implementations doing client side encryption
	// and decryption on top of s3 must adhere to.
	SecureS3Client interface {
		// Get fetches an object from S3 and decrypts it
		Get(bucket string, key string) (io.ReadCloser, error)
		// Put encrypts and stores the object in S3
		Put(bucket string, key string, body io.Reader) error
		// List lists objects with prefix
		List(bucket string, prefix string) (map[string]int64, error)
	}

	// s3Client is an implementation of SecureS3Client
	s3Client struct {
		s3           *s3.S3
		uploader     *s3manager.Uploader
		credProvider credentials.Provider
		aesKey       []byte
	}

	// SecureS3ClientError indicates any
	//  error from SecureS3Client
	SecureS3ClientError struct {
		msg string
	}
)

const (
	ivMetadataKey      = "Encryptioniv"   // s3 object metadata
	encTypeMetadataKey = "Encryptiontype" // s3 object metadata
)

const encTypeAES256GCM = "AES256_GCM" // Only encryptionType we support
const aesIvSize = 12

// ErrMissingEncType indicates the object metadata missing encryptionType
var ErrMissingEncType = newSecureS3ClientError("Object metadata missing encryptionType")

// ErrMissingIV indicates object metadata missing encryptionIv
var ErrMissingIV = newSecureS3ClientError("Object metadata missing encryptionIV")

// newSecureS3ClientError returns a new SecureS3ClientError object
func newSecureS3ClientError(msg string) error {
	return &SecureS3ClientError{
		msg: msg,
	}
}

// Error implements error interface
func (c *SecureS3ClientError) Error() string {
	return c.msg
}

// NewSecureS3Client creates and returns a new SecureS3Client
// The returned client will do client side encryption and
// decryption for all objects stored / fetched from S3. Only
// objects previously stored using this client can be retrieved.
// For decryption, its the caller's responsiblility to make sure
// the same aes key used for encryption is supplied.
func NewSecureS3Client(region string, credProvider credentials.Provider, aesKey []byte) SecureS3Client {
	creds := credentials.NewCredentials(credProvider)
	cfg := aws.NewConfig().WithRegion(region).WithCredentials(creds)
	sess := session.New(cfg)
	s3Svc := s3.New(sess)
	uploader := s3manager.NewUploaderWithClient(s3Svc)

	return &s3Client{
		credProvider: credProvider,
		aesKey:       aesKey,
		s3:           s3Svc,
		uploader:     uploader,
	}
}

// Put puts an object into S3 after encryption
func (client *s3Client) Put(bucket string, key string, body io.Reader) error {

	iv, err := generateRandom(aesIvSize)
	if err != nil {
		return err
	}

	metadata := make(map[string]*string, 2)
	metadata[ivMetadataKey] = aws.String(hex.EncodeToString(iv))
	metadata[encTypeMetadataKey] = aws.String(encTypeAES256GCM)

	encryptedStream, err := NewAESEncryptor(client.aesKey, iv, body)
	if err != nil {
		return nil
	}

	input := &s3manager.UploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Metadata: metadata,
		Body:     encryptedStream,
	}

	_, err = client.uploader.Upload(input)
	if err != nil {
		return err
	}
	return nil
}

// Get gets an object from S3 and decrypts it
func (client *s3Client) Get(bucket string, key string) (io.ReadCloser, error) {

	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := client.s3.GetObject(input)
	if err != nil {
		return nil, err
	}

	iv, err := client.readIV(output.Metadata)
	if err != nil {
		output.Body.Close()
		return nil, err
	}

	decryptStream, err := NewAESDecryptor(client.aesKey, iv, output.Body)
	if err != nil {
		output.Body.Close()
		return nil, err
	}

	return decryptStream, nil
}

// List lists all objects with given prefix
func (client *s3Client) List(bucket string, prefix string) (map[string]int64, error) {

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	output, err := client.s3.ListObjectsV2(input)
	if err != nil {
		return nil, err
	}

	result := make(map[string]int64, len(output.Contents))
	for _, obj := range output.Contents {
		result[*obj.Key] = *obj.Size
	}
	return result, nil
}

func (client *s3Client) readIV(metadata map[string]*string) ([]byte, error) {

	encType, ok := metadata[encTypeMetadataKey]
	if !ok {
		return nil, ErrMissingEncType
	}

	if *encType != encTypeAES256GCM {
		return nil, newSecureS3ClientError("Unknown ecryption type " + *encType)
	}

	ivStr, ok := metadata[ivMetadataKey]
	if !ok {
		return nil, ErrMissingIV
	}

	iv, err := hex.DecodeString(*ivStr)
	if err != nil {
		return nil, err
	}

	return iv, nil
}

func generateRandom(size int) ([]byte, error) {
	bytes := make([]byte, size)
	_, err := io.ReadFull(rand.Reader, bytes[:])
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
