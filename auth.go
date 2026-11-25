// S3 authentication: pluggable backends behind one interface, modeled
// on Ceph RGW's user model (uid, full name, email, rotatable
// access/secret key pairs, suspend). The default backend ("builtin")
// stores users and keys in the catalog DB and is managed with the
// `tapeberg user` / `tapeberg key` commands. TAPEBERG_AUTH selects the
// backend; LDAP and Active Directory drop in later as new
// implementations of the same interface.
package main

import (
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
)

// ErrUnknownAccessKey maps to S3 InvalidAccessKeyId.
var ErrUnknownAccessKey = errors.New("unknown access key")

// AuthUser is the resolved caller, whatever the backend.
type AuthUser struct {
	ID       int64 // 0 = not a DB user (anonymous dev mode / static key)
	UserID   string
	FullName string
	Email    string
	IsAdmin  bool
	Suspended bool
}

// Authenticator verifies SigV4 access keys.
type Authenticator interface {
	// Enabled reports whether requests must authenticate. False =
	// unsigned dev mode (the legacy bootstrap: no users in the DB and
	// no static key configured).
	Enabled() bool
	// LookupSecret returns the secret for an access key plus the user
	// it belongs to. ErrUnknownAccessKey rejects the key.
	LookupSecret(accessKey string) (secret string, user AuthUser, err error)
	// Name identifies the backend (doctor output, logs).
	Name() string
}

// NewAuthenticator builds the configured auth backend.
func NewAuthenticator(cfg Config, db *sql.DB) (Authenticator, error) {
	switch cfg.Auth {
	case "", "builtin":
		return &BuiltinAuth{cfg: cfg, db: db}, nil
	default:
		return nil, fmt.Errorf("auth backend %q is not built yet (builtins: builtin)", cfg.Auth)
	}
}

// BuiltinAuth authenticates against s3_users/s3_keys. While the users
// table is empty it falls back to the static TAPEBERG_S3_KEY/SECRET
// pair (or unsigned dev mode when unset) — the first created user
// switches the endpoint to real DB auth.
type BuiltinAuth struct {
	cfg Config
	db  *sql.DB
}

func (b *BuiltinAuth) Name() string { return "builtin" }

func (b *BuiltinAuth) Enabled() bool {
	if b.cfg.S3Key != "" {
		return true
	}
	var n int
	if err := b.db.QueryRow(`SELECT count(*) FROM s3_users`).Scan(&n); err != nil {
		return true // fail closed on catalog trouble
	}
	return n > 0
}

func (b *BuiltinAuth) LookupSecret(accessKey string) (string, AuthUser, error) {
	var (
		secret string
		user   AuthUser
	)
	err := b.db.QueryRow(`
		SELECT k.secret_key, u.id, u.user_id, u.full_name, COALESCE(u.email,''),
		       u.is_admin, u.suspended
		FROM s3_keys k JOIN s3_users u ON u.id = k.user_id
		WHERE k.access_key = $1`, accessKey).
		Scan(&secret, &user.ID, &user.UserID, &user.FullName, &user.Email,
			&user.IsAdmin, &user.Suspended)
	if errors.Is(err, sql.ErrNoRows) {
		// No DB users yet → legacy static-key bootstrap (an admin).
		var users int
		if uerr := b.db.QueryRow(`SELECT count(*) FROM s3_users`).Scan(&users); uerr == nil &&
			users == 0 && b.cfg.S3Key != "" && accessKey == b.cfg.S3Key {
			return b.cfg.S3Secret, AuthUser{UserID: "static", IsAdmin: true}, nil
		}
		return "", user, ErrUnknownAccessKey
	}
	if err != nil {
		return "", user, err
	}
	return secret, user, nil
}

// ---- key generation (AWS-style shapes, like radosgw-admin) ----

const accessAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const secretAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

func randomString(n int, alphabet string) (string, error) {
	out := make([]byte, n)
	max := big.NewInt(int64(len(alphabet)))
	for i := range out {
		idx, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		out[i] = alphabet[idx.Int64()]
	}
	return string(out), nil
}

func genAccessKey() (string, error) { return randomString(20, accessAlphabet) }
func genSecretKey() (string, error) { return randomString(40, secretAlphabet) }

// ---- catalog CRUD used by the `user`/`key` commands ----

func dbUserAdd(db *sql.DB, uid, fullName, email string, isAdmin bool) error {
	_, err := db.Exec(`INSERT INTO s3_users (user_id, full_name, email, is_admin)
		VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO NOTHING`, uid, fullName, email, isAdmin)
	return err
}

func dbUserExists(db *sql.DB, uid string) (bool, error) {
	var n int
	err := db.QueryRow(`SELECT count(*) FROM s3_users WHERE user_id = $1`, uid).Scan(&n)
	return n > 0, err
}

func dbUserRm(db *sql.DB, uid string) error {
	_, err := db.Exec(`DELETE FROM s3_users WHERE user_id = $1`, uid)
	return err
}

func dbUserSetSuspended(db *sql.DB, uid string, suspended bool) error {
	res, err := db.Exec(`UPDATE s3_users SET suspended = $2 WHERE user_id = $1`, uid, suspended)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("user %q not found", uid)
	}
	return nil
}

// dbKeyAdd generates and stores a fresh key pair for the user.
func dbKeyAdd(db *sql.DB, uid string) (access, secret string, err error) {
	access, err = genAccessKey()
	if err != nil {
		return "", "", err
	}
	secret, err = genSecretKey()
	if err != nil {
		return "", "", err
	}
	_, err = db.Exec(`INSERT INTO s3_keys (user_id, access_key, secret_key)
		SELECT id, $2, $3 FROM s3_users WHERE user_id = $1`, uid, access, secret)
	if err != nil {
		return "", "", err
	}
	return access, secret, nil
}

func dbKeyRm(db *sql.DB, access string) error {
	res, err := db.Exec(`DELETE FROM s3_keys WHERE access_key = $1`, access)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("key %q not found", access)
	}
	return nil
}

type KeyRow struct {
	Access string
	Secret string
}

func dbUserKeys(db *sql.DB, uid string) ([]KeyRow, error) {
	rows, err := db.Query(`SELECT k.access_key, k.secret_key
		FROM s3_keys k JOIN s3_users u ON u.id = k.user_id
		WHERE u.user_id = $1 ORDER BY k.id`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []KeyRow
	for rows.Next() {
		var k KeyRow
		if err := rows.Scan(&k.Access, &k.Secret); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

type UserRow struct {
	UID       string
	FullName  string
	Email     string
	IsAdmin   bool
	Suspended bool
	Keys      int
}

func dbUserList(db *sql.DB) ([]UserRow, error) {
	rows, err := db.Query(`
		SELECT u.user_id, u.full_name, COALESCE(u.email,''), u.is_admin, u.suspended,
		       (SELECT count(*) FROM s3_keys k WHERE k.user_id = u.id)
		FROM s3_users u ORDER BY u.user_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UserRow
	for rows.Next() {
		var u UserRow
		if err := rows.Scan(&u.UID, &u.FullName, &u.Email, &u.IsAdmin, &u.Suspended, &u.Keys); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func dbUserGet(db *sql.DB, uid string) (*UserRow, error) {
	u := &UserRow{}
	err := db.QueryRow(`
		SELECT u.user_id, u.full_name, COALESCE(u.email,''), u.is_admin, u.suspended,
		       (SELECT count(*) FROM s3_keys k WHERE k.user_id = u.id)
		FROM s3_users u WHERE u.user_id = $1`, uid).
		Scan(&u.UID, &u.FullName, &u.Email, &u.IsAdmin, &u.Suspended, &u.Keys)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("user %q: %w", uid, ErrNotFound)
	}
	return u, err
}
