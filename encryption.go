package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"github.axa.com/axa-partners-clp/mrt-shared/logging"
	"io"
	"log"
)

type Client interface {
	Encrypt(payload string) (string, error)
	Decrypt(payload string) (string, error)
}

type ClientImpl struct {
	c      cipher.Block
	logger logging.Client
}

func New(key string, logger logging.Client) *ClientImpl {
	secretKey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		if logger != nil {
			logger.Fatalf("cannot read AES key: %v", err)
		} else {
			log.Fatalf("cannot read AES key: %v", err)
		}
	}

	c, err := aes.NewCipher(secretKey)
	if err != nil {
		if logger != nil {
			logger.Fatalf("cannot create AES cipher: %v", err)
		} else {
			log.Fatalf("cannot create AES cipher: %v", err)
		}
	}

	return &ClientImpl{
		c:      c,
		logger: logger,
	}
}

func (e ClientImpl) Encrypt(payload string) (string, error) {
	payloadBytes := []byte(payload)

	gcm, err := cipher.NewGCM(e.c)
	if err != nil {
		e.printf("cannot create GCM encoder: %v", err)
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	cipherText := gcm.Seal(nil, nonce, payloadBytes, nil)
	cipherText = append(nonce, cipherText...)

	return base64.StdEncoding.EncodeToString(cipherText), nil
}

func (e ClientImpl) Decrypt(payload string) (string, error) {
	cipherText, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(e.c)
	if err != nil {
		e.printf("cannot create GCM decoder: %v", err)
		return "", err
	}

	nonce := cipherText[:gcm.NonceSize()]
	cipherText = cipherText[gcm.NonceSize():]

	cipherText, err = gcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		e.printf("cannot decrypt data: %v", err)
		return "", err
	}

	return string(cipherText), nil
}

func (e ClientImpl) printf(format string, args ...any) {
	if e.logger != nil {
		e.logger.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}
