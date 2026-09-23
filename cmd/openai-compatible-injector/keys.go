package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"openai-compatible-injector/internal/auth"
)

// Partner key management, a CLI beside the proxy — deliberately not an HTTP
// surface on it: key lifecycle is an operator action with operator
// authentication (shell access), never a network-reachable endpoint waiting
// to be found.
//
//	openai-compatible-injector keys create  -partner <id> [-database <url>]
//	openai-compatible-injector keys list                [-database <url>]
//	openai-compatible-injector keys revoke  -key-id <id> [-database <url>]
//
// The database URL falls back to OAICR_AUTH_DATABASE_URL when the flag is
// absent, so the proxy and the management CLI share one configuration
// source. Plaintext tokens appear exactly once — on create's stdout — and
// never again: the store keeps only the SHA-256 digest, list prints no
// secret material (the record type structurally carries no hash), and
// nothing in this file routes key material through a logger.

const keysOperationTimeout = 30 * time.Second

func keysCommand(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "keys requires an operation: create, list or revoke")
		return 2
	}
	var code int
	switch args[0] {
	case "create":
		code = keysCreate(args[1:])
	case "list":
		code = keysList(args[1:])
	case "revoke":
		code = keysRevoke(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown keys operation %q\n", args[0])
		code = 2
	}
	return code
}

// keysDatabase resolves the connection string: flag first, environment as
// the fallback, neither is an error. A bad URL fails at store open.
func keysDatabase(database string) (string, int) {
	if database != "" {
		return database, 0
	}
	if env := os.Getenv("OAICR_AUTH_DATABASE_URL"); env != "" {
		return env, 0
	}
	fmt.Fprintln(os.Stderr, "no key store: pass -database or set OAICR_AUTH_DATABASE_URL")
	return "", 2
}

func keysCreate(args []string) int {
	fs := flag.NewFlagSet("keys create", flag.ContinueOnError)
	partner := fs.String("partner", "", "partner identifier the key belongs to (required)")
	database := fs.String("database", "", "PostgreSQL connection string (or OAICR_AUTH_DATABASE_URL)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*partner) == "" {
		fmt.Fprintln(os.Stderr, "keys create requires -partner")
		return 2
	}
	url, code := keysDatabase(*database)
	if code != 0 {
		return code
	}

	// Mint before touching the store: the token exists exactly once in
	// memory and is either persisted as its digest or discarded.
	token, err := auth.GenerateToken()
	if err != nil {
		fmt.Fprintf(os.Stderr, "key generation failed: %v\n", err)
		return 1
	}
	keyID, err := auth.GenerateKeyID()
	if err != nil {
		fmt.Fprintf(os.Stderr, "key generation failed: %v\n", err)
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), keysOperationTimeout)
	defer cancel()
	store, err := auth.NewPGStore(ctx, url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "key store unavailable\n")
		return 1
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = store.Close(closeCtx)
	}()

	rec := auth.KeyRecord{
		KeyID:     keyID,
		PartnerID: strings.TrimSpace(*partner),
		Status:    auth.StatusActive,
		CreatedAt: time.Now().UTC(),
	}
	if err := store.CreateKey(ctx, rec, auth.HashToken(token)); err != nil {
		fmt.Fprintf(os.Stderr, "key creation failed\n")
		return 1
	}
	fmt.Printf("key created\n")
	fmt.Printf("  key_id:     %s\n", keyID)
	fmt.Printf("  partner_id: %s\n", rec.PartnerID)
	fmt.Printf("  token:      %s\n", token)
	fmt.Printf("\nThe token is shown exactly once and cannot be retrieved again —\nstore it now. The key store holds only its SHA-256 digest.\n")
	return 0
}

func keysList(args []string) int {
	fs := flag.NewFlagSet("keys list", flag.ContinueOnError)
	database := fs.String("database", "", "PostgreSQL connection string (or OAICR_AUTH_DATABASE_URL)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	url, code := keysDatabase(*database)
	if code != 0 {
		return code
	}

	ctx, cancel := context.WithTimeout(context.Background(), keysOperationTimeout)
	defer cancel()
	store, err := auth.NewPGStore(ctx, url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "key store unavailable\n")
		return 1
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = store.Close(closeCtx)
	}()

	keys, err := store.ListKeys(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "key listing failed\n")
		return 1
	}
	fmt.Println("key_id\tpartner_id\tstatus\tcreated_at\trevoked_at\tlast_used_at")
	for _, rec := range keys {
		fmt.Printf("%s\t%s\t%s\t%s\t%s\t%s\n",
			rec.KeyID, rec.PartnerID, rec.Status,
			formatTime(rec.CreatedAt), formatOptTime(rec.RevokedAt), formatOptTime(rec.LastUsedAt))
	}
	return 0
}

func keysRevoke(args []string) int {
	fs := flag.NewFlagSet("keys revoke", flag.ContinueOnError)
	keyID := fs.String("key-id", "", "key identifier to revoke (required)")
	database := fs.String("database", "", "PostgreSQL connection string (or OAICR_AUTH_DATABASE_URL)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*keyID) == "" {
		fmt.Fprintln(os.Stderr, "keys revoke requires -key-id")
		return 2
	}
	url, code := keysDatabase(*database)
	if code != 0 {
		return code
	}

	ctx, cancel := context.WithTimeout(context.Background(), keysOperationTimeout)
	defer cancel()
	store, err := auth.NewPGStore(ctx, url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "key store unavailable\n")
		return 1
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = store.Close(closeCtx)
	}()

	revoked, err := store.RevokeKey(ctx, strings.TrimSpace(*keyID))
	if err != nil {
		fmt.Fprintf(os.Stderr, "revocation failed\n")
		return 1
	}
	if !revoked {
		fmt.Fprintf(os.Stderr, "no active key with that key_id (unknown or already revoked)\n")
		return 1
	}
	fmt.Printf("key revoked: %s\n", strings.TrimSpace(*keyID))
	fmt.Printf("Requests presenting it are denied within %s (the positive cache bound).\n", auth.PositiveTTL())
	return 0
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func formatOptTime(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return formatTime(*t)
}
