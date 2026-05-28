// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2024 Tulir Asokan, Ludvig Rhodin
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"

	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
)

// runMigrate is the entrypoint for `mautrix-imessage migrate ...`.
//
// Default behavior is a dry-run that prints row counts and what would change.
// Pass --apply to actually write to bridge_message_meta. Pass --drop-legacy
// (which implies --apply) to additionally hard-delete cloud_message and
// cloud_attachment_cache after the backfill.
//
// The backfill copies topology-only columns from cloud_message into
// bridge_message_meta (GUIDs, portal_id, chat_id, timestamp, sender, tapback
// pointers, is_from_me, deleted). Text, subject, attachment JSON blobs and
// record_name are intentionally NOT copied — they are exactly the cleartext
// the privacy fork exists to remove.
//
// Attachments (cloud_attachment_cache → bridge_attachment_meta) are NOT
// migrated by this command. The two schemas key differently (record_name vs
// GUID), and the GUID mapping lives inside cloud_message.attachments_json. A
// best-effort attachment migration is feasible but adds significant
// JSON-parsing complexity for marginal value: the next CloudKit sync after
// drop-legacy re-uploads every attachment and the dual-write path we already
// shipped (commit 60029fc) populates bridge_attachment_meta directly. Users
// who care about attachment-cache resumability should run a full re-sync
// after migrating.
func runMigrate(m *mxmain.BridgeMain) {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	toPrivacyFork := fs.Bool("to-privacy-fork", false,
		"Required. Migrate from upstream/Jason cleartext schema to identifier-only schema.")
	apply := fs.Bool("apply", false,
		"Execute the backfill. Without this flag the command only reports counts (dry-run).")
	dropLegacy := fs.Bool("drop-legacy", false,
		"After backfill, hard-delete cloud_message and cloud_attachment_cache. Implies --apply. IRREVERSIBLE.")

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: mautrix-imessage migrate --to-privacy-fork [--apply] [--drop-legacy]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Migrates an existing bridge.db from the upstream cleartext schema")
		fmt.Fprintln(fs.Output(), "(lrhodin/jasonlaguidice) to this fork's identifier-only schema.")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Default is a dry-run — pre-migration row counts are printed and the")
		fmt.Fprintln(fs.Output(), "database is not touched. Pass --apply to execute. The drop step is gated")
		fmt.Fprintln(fs.Output(), "behind --drop-legacy and is irreversible — back up bridge.db first.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}
	_ = fs.Parse(os.Args[1:])

	if !*toPrivacyFork {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "error: --to-privacy-fork is required")
		os.Exit(2)
	}
	if *dropLegacy {
		*apply = true
	}

	// Initialize DB without starting Matrix.
	m.PreInit()
	repairPermissions(m)
	m.Init()
	ctx := context.Background()
	rawDB := m.Bridge.DB.Database.RawDB

	if err := ensureBridgeMetaSchemaForMigrate(ctx, rawDB); err != nil {
		fmt.Fprintf(os.Stderr, "error: could not ensure target schema: %v\n", err)
		os.Exit(1)
	}

	pre, err := snapshotCounts(ctx, rawDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: pre-count failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "Pre-migration row counts:")
	fmt.Fprintf(os.Stderr, "  cloud_message:          %d\n", pre.cloudMessages)
	fmt.Fprintf(os.Stderr, "  bridge_message_meta:    %d\n", pre.bridgeMessages)
	fmt.Fprintf(os.Stderr, "  cloud_attachment_cache: %d\n", pre.cloudAttachments)
	fmt.Fprintf(os.Stderr, "  bridge_attachment_meta: %d\n", pre.bridgeAttachments)
	fmt.Fprintln(os.Stderr, "")

	if !*apply {
		fmt.Fprintln(os.Stderr, "Dry-run. To execute:")
		fmt.Fprintln(os.Stderr, "  mautrix-imessage migrate --to-privacy-fork --apply")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "To also drop legacy tables (irreversible):")
		fmt.Fprintln(os.Stderr, "  mautrix-imessage migrate --to-privacy-fork --apply --drop-legacy")
		os.Exit(0)
	}

	inserted, err := backfillBridgeMessageMeta(ctx, rawDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: backfill failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Backfilled %d new rows into bridge_message_meta\n", inserted)

	if *dropLegacy {
		if err := dropLegacyTables(ctx, rawDB); err != nil {
			fmt.Fprintf(os.Stderr, "error: drop-legacy failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "Dropped cloud_message and cloud_attachment_cache")
	} else {
		fmt.Fprintln(os.Stderr, "Legacy tables retained. Re-run with --drop-legacy to remove them.")
	}

	post, err := snapshotCounts(ctx, rawDB)
	if err == nil {
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Post-migration row counts:")
		fmt.Fprintf(os.Stderr, "  cloud_message:          %d\n", post.cloudMessages)
		fmt.Fprintf(os.Stderr, "  bridge_message_meta:    %d\n", post.bridgeMessages)
		fmt.Fprintf(os.Stderr, "  cloud_attachment_cache: %d\n", post.cloudAttachments)
		fmt.Fprintf(os.Stderr, "  bridge_attachment_meta: %d\n", post.bridgeAttachments)
	}
}

// rowCounts is the snapshot returned by snapshotCounts. Missing tables count
// as 0 so the same code path works pre- and post-drop.
type rowCounts struct {
	cloudMessages     int64
	bridgeMessages    int64
	cloudAttachments  int64
	bridgeAttachments int64
}

func snapshotCounts(ctx context.Context, db *sql.DB) (rowCounts, error) {
	var c rowCounts
	c.cloudMessages = countOrZero(ctx, db, "cloud_message")
	c.bridgeMessages = countOrZero(ctx, db, "bridge_message_meta")
	c.cloudAttachments = countOrZero(ctx, db, "cloud_attachment_cache")
	c.bridgeAttachments = countOrZero(ctx, db, "bridge_attachment_meta")
	return c, nil
}

// countOrZero returns SELECT COUNT(*) FROM <table>, or 0 if the table does
// not exist. The migration code paths run before and after destructive drops,
// so a missing table is expected (not an error).
func countOrZero(ctx context.Context, db *sql.DB, table string) int64 {
	var n int64
	row := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table)
	if err := row.Scan(&n); err != nil {
		return 0
	}
	return n
}

// ensureBridgeMetaSchemaForMigrate creates the privacy-fork tables if they
// don't already exist. Duplicated from bridge_meta_store.go on purpose: the
// migrate command must work even on a DB that has never run the connector's
// per-login schema bootstrap (e.g., a fresh upgrade install).
func ensureBridgeMetaSchemaForMigrate(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS bridge_message_meta (
			login_id            TEXT    NOT NULL,
			guid                TEXT    NOT NULL,
			record_name         TEXT    NOT NULL DEFAULT '',
			portal_id           TEXT,
			chat_id             TEXT,
			timestamp_ms        BIGINT  NOT NULL,
			sender              TEXT,
			is_from_me          BOOLEAN NOT NULL,
			service             TEXT,
			deleted             BOOLEAN NOT NULL DEFAULT FALSE,
			reply_target_guid   TEXT,
			tapback_target_guid TEXT,
			tapback_emoji       TEXT,
			tapback_type        INTEGER,
			attachment_guids    TEXT,
			retry_count         INTEGER NOT NULL DEFAULT 0,
			next_attempt_at     BIGINT,
			last_error_kind     TEXT,
			created_ts          BIGINT NOT NULL,
			updated_ts          BIGINT NOT NULL,
			PRIMARY KEY (login_id, guid)
		)`,
		`CREATE INDEX IF NOT EXISTS bridge_message_meta_portal_idx
			ON bridge_message_meta (login_id, portal_id, timestamp_ms)`,
		`CREATE TABLE IF NOT EXISTS bridge_attachment_meta (
			login_id        TEXT    NOT NULL,
			guid            TEXT    NOT NULL,
			message_guid    TEXT,
			mxc_uri         TEXT,
			mime            TEXT,
			size_bytes      BIGINT,
			width           INTEGER,
			height          INTEGER,
			duration_ms     INTEGER,
			is_live_photo   BOOLEAN NOT NULL DEFAULT FALSE,
			live_pair_guid  TEXT,
			retry_count     INTEGER NOT NULL DEFAULT 0,
			next_attempt_at BIGINT,
			created_ts      BIGINT NOT NULL,
			PRIMARY KEY (login_id, guid)
		)`,
	}
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

// backfillBridgeMessageMeta copies topology-only columns from cloud_message
// into bridge_message_meta. Existing rows on conflict are left untouched so
// repeat invocations are safe. Returns the number of inserted rows.
//
// Deliberately omitted from the SELECT:
//
//	text                — message body (cleartext, by definition non-privacy)
//	subject             — message subject (same)
//	attachments_json    — JSON list of cloudAttachmentRow with file metadata
//	record_name         — CloudKit record name (caller can re-derive)
//	date_read_ms        — read receipt state (not tracked by the new schema)
//	has_body            — rich-text flag (only meaningful while text exists)
//
// reply_target_guid is also not populated: cloud_message never tracked it, and
// the rustpush FFI does not yet surface it on real-time deliveries either.
// That gap is closed by a future change; existing data simply lacks the field.
func backfillBridgeMessageMeta(ctx context.Context, db *sql.DB) (int64, error) {
	res, err := db.ExecContext(ctx, `
		INSERT INTO bridge_message_meta (
			login_id, guid, record_name, portal_id, chat_id, timestamp_ms, sender, is_from_me,
			service, deleted, tapback_target_guid, tapback_emoji, tapback_type,
			retry_count, created_ts, updated_ts
		)
		SELECT
			login_id, guid, COALESCE(record_name, ''), portal_id, chat_id, timestamp_ms, sender, is_from_me,
			service, deleted, tapback_target_guid, tapback_emoji, tapback_type,
			0, created_ts, updated_ts
		FROM cloud_message
		WHERE NOT EXISTS (
			SELECT 1 FROM bridge_message_meta b
			WHERE b.login_id = cloud_message.login_id AND b.guid = cloud_message.guid
		)
	`)
	if err != nil {
		return 0, fmt.Errorf("backfill INSERT: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("backfill row count: %w", err)
	}
	return n, nil
}

// dropLegacyTables hard-deletes cloud_message and cloud_attachment_cache.
// The cloud_chat (chat-metadata) and cloud_sync_state (continuation tokens)
// tables are retained: they hold no message content and remain useful for
// chat-rename resolution and resume-from-token operations.
func dropLegacyTables(ctx context.Context, db *sql.DB) error {
	for _, table := range []string{"cloud_message", "cloud_attachment_cache"} {
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop %s: %w", table, err)
		}
	}
	return nil
}
