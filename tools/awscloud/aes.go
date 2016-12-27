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
	"crypto/aes"
	"crypto/cipher"
	"io"
)

type (
	// aesCryptor is a base type that
	// the Encryptor/Decryptor depends on
	aesCryptor struct {
		encKey []byte
		encIv  []byte
		gcm    cipher.AEAD
		buf    []byte // staging buffer where we store raw data before enc/decrypt
		bufOff int    // staging buffer offset, data starts from this offset
		bufRem int    // staging buffer remaining, length of the data within the buffer
		err    error  // if the cryptor is in error state, this will be non-nil
	}

	// AESEncryptor is an implementation of
	// io.Reader that takes a plain text Reader
	// and converts it into AES cipher text
	AESEncryptor struct {
		aesCryptor
		reader io.Reader // reads plaintext
	}

	// AESDecryptor is an implementation of
	// io.Reader that takes a cipher text reader
	// and converts it into plain text
	AESDecryptor struct {
		aesCryptor
		reader io.ReadCloser // reads cipher text
	}
)

// ciperChunkSize is the basic encrypt/decrypt block size
// AEAD_GCM ciphers are not block ciphers, so they don't
// need padding. So this chunking is just to support
// streaming data without knowing the whole file size
// before hand.
const cipherChunkSize = 4 * 1024

// newAESCryptor returns a new cryptor instance
func newAESCryptor(encKey []byte, encIv []byte) (*aesCryptor, error) {
	gcm, err := createCipher(encKey)
	if err != nil {
		return nil, err
	}

	bufSize := cipherChunkSize + gcm.Overhead()

	return &aesCryptor{
		encKey: encKey,
		encIv:  encIv,
		gcm:    gcm,
		buf:    make([]byte, bufSize),
	}, nil
}

// NewAESEncryptor returns a new encryptor stream that
// encrypts using the given key and iv
func NewAESEncryptor(encKey []byte, encIv []byte, reader io.Reader) (*AESEncryptor, error) {
	cryptor, err := newAESCryptor(encKey, encIv)
	if err != nil {
		return nil, err
	}
	return &AESEncryptor{
		aesCryptor: *cryptor,
		reader:     reader,
	}, nil
}

// NewAESDecryptor returns a new decryptor stream that
// decrypts using the given key and iv
func NewAESDecryptor(encKey []byte, encIv []byte, reader io.ReadCloser) (*AESDecryptor, error) {
	cryptor, err := newAESCryptor(encKey, encIv)
	if err != nil {
		return nil, err
	}
	return &AESDecryptor{
		aesCryptor: *cryptor,
		reader:     reader,
	}, nil
}

// Read reads plain text and returns cipher text
func (e *AESEncryptor) Read(p []byte) (int, error) {
	for {

		if len(p) < 1 || e.err != nil {
			return 0, e.err
		}

		if e.bufRem > 0 {
			src := e.buf[e.bufOff : e.bufOff+e.bufRem]
			n := copy(p, src)
			e.bufRem -= n
			e.bufOff += n
			return n, nil
		}

		e.bufOff = 0

		n, err := io.ReadFull(e.reader, e.buf[:cipherChunkSize])

		if n > 0 {
			plaintext := e.buf[:n]
			out := e.gcm.Seal(plaintext[:0], e.encIv, plaintext, []byte{})
			e.bufRem = len(out)
		} else {
			if err == io.ErrUnexpectedEOF {
				e.err = io.EOF
				return 0, io.EOF
			}
			e.err = err
			return 0, err
		}
	}
}

// Read reads cipher text and returns plain text
func (e *AESDecryptor) Read(p []byte) (int, error) {

	var n int
	var err error

	for {

		if len(p) < 1 || e.err != nil {
			return 0, e.err
		}

		if e.bufRem > 0 {
			src := e.buf[e.bufOff : e.bufOff+e.bufRem]
			n = copy(p, src)
			e.bufRem -= n
			e.bufOff += n
			return n, nil
		}

		e.bufOff = 0

		n, err = io.ReadFull(e.reader, e.buf[:])
		if n > 0 && n < e.gcm.Overhead() {
			e.err = io.ErrUnexpectedEOF
			return 0, e.err
		}

		if n > 0 {
			ciphertext := e.buf[:n]
			out, err1 := e.gcm.Open(ciphertext[:0], e.encIv, ciphertext, []byte{})
			if err1 != nil {
				e.err = err1
				return 0, err1
			}
			e.bufRem = len(out)
			continue
		}

		if err != nil {
			if err == io.ErrUnexpectedEOF {
				e.err = io.EOF
				return 0, io.EOF
			}
			e.err = err
			return 0, err
		}
	}
}

// Close closes the underlying stream
func (e *AESDecryptor) Close() error {
	return e.reader.Close()
}

func createCipher(key []byte) (cipher.AEAD, error) {
	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(aesBlock)
}

func min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}
