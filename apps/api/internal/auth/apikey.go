package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

var (
	ErrInvalidAPIKey = errors.New("invalid api key")
)

type StoredAPIKey struct {
	Prefix string
	Hash   string
}

type KeyRecord struct {
	ID        int64
	UserID    *int64
	Name      string
	Prefix    string
	Hash      string
	RevokedAt *time.Time
	ExpiresAt *time.Time
}

type Principal struct {
	KeyID   int64
	UserID  *int64
	Name    string
	Prefix  string
}

type Authenticator interface {
	Authenticate(ctx context.Context, rawKey string) (*Principal, error)
}

type KeyStore interface {
	GetAPIKeyByPrefix(ctx context.Context, prefix string) (KeyRecord, error)
	TouchAPIKeyLastUsed(ctx context.Context, id int64, lastUsedAt time.Time) error
}

type Service struct {
	store  KeyStore
	pepper string
	now    func() time.Time
}

func NewService(store KeyStore, pepper string) *Service {
	return &Service{
		store:  store,
		pepper: pepper,
		now:    time.Now,
	}
}

func GenerateAPIKey(pepper string, random io.Reader) (string, StoredAPIKey, error) {
	prefix, err := randomHex(random, 4)
	if err != nil {
		return "", StoredAPIKey{}, err
	}
	secret, err := randomHex(random, 24)
	if err != nil {
		return "", StoredAPIKey{}, err
	}

	key := prefix + "." + secret
	return key, StoredAPIKey{
		Prefix: prefix,
		Hash:   HashAPIKey(key, pepper),
	}, nil
}

func HashAPIKey(rawKey, pepper string) string {
	sum := sha256.Sum256([]byte(pepper + ":" + rawKey))
	return hex.EncodeToString(sum[:])
}

func ValidateAPIKey(rawKey, pepper string, stored StoredAPIKey) bool {
	prefix, _, ok := SplitAPIKey(rawKey)
	if !ok || prefix != stored.Prefix {
		return false
	}

	actual := HashAPIKey(rawKey, pepper)
	return subtle.ConstantTimeCompare([]byte(actual), []byte(stored.Hash)) == 1
}

func SplitAPIKey(rawKey string) (string, string, bool) {
	prefix, secret, ok := strings.Cut(strings.TrimSpace(rawKey), ".")
	if !ok || prefix == "" || secret == "" {
		return "", "", false
	}
	return prefix, secret, true
}

func (s *Service) Authenticate(ctx context.Context, rawKey string) (*Principal, error) {
	prefix, _, ok := SplitAPIKey(rawKey)
	if !ok {
		return nil, ErrInvalidAPIKey
	}

	record, err := s.store.GetAPIKeyByPrefix(ctx, prefix)
	if err != nil {
		return nil, ErrInvalidAPIKey
	}

	if record.RevokedAt != nil {
		return nil, ErrInvalidAPIKey
	}
	if record.ExpiresAt != nil && record.ExpiresAt.Before(s.now()) {
		return nil, ErrInvalidAPIKey
	}
	if !ValidateAPIKey(rawKey, s.pepper, StoredAPIKey{Prefix: record.Prefix, Hash: record.Hash}) {
		return nil, ErrInvalidAPIKey
	}

	_ = s.store.TouchAPIKeyLastUsed(ctx, record.ID, s.now())

	return &Principal{
		KeyID:  record.ID,
		UserID: record.UserID,
		Name:   record.Name,
		Prefix: record.Prefix,
	}, nil
}

func randomHex(reader io.Reader, size int) (string, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
