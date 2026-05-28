package connector

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// bridgeMetaStore holds the identifier-only persistence layer for the privacy
// fork. Where cloudBackfillStore (the upstream/Jason scheme) persists message
// text, subject lines and attachment JSON blobs, this store only persists
// GUIDs, MXIDs, mxc:// pointers, timestamps and soft-delete tombstones — the
// minimum the bridge needs to dedup against future CloudKit replays and to
// resolve replies/tapbacks across bridge restarts.
//
// During the fork transition the new tables run *alongside* the old ones; a
// follow-up commit migrates readers/writers over and drops cloud_message +
// cloud_attachment_cache for good.
type bridgeMetaStore struct {
	db      *dbutil.Database
	loginID networkid.UserLoginID
}

func newBridgeMetaStore(db *dbutil.Database, loginID networkid.UserLoginID) *bridgeMetaStore {
	return &bridgeMetaStore{db: db, loginID: loginID}
}

// bridgeMessageMetaRow is the in-memory shape of a bridge_message_meta row.
// No content fields — text, subject, attachment blobs intentionally absent.
//
// RecordName is the opaque CloudKit record identifier. We persist it because
// several legacy code paths (ghost-receipt suppression, restored-chat seeding)
// distinguish CloudKit-imported messages from real-time-inserted stubs by
// checking record_name <> ''. It's a pointer, not content — same privacy
// posture as the GUID or the mxc:// URI.
type bridgeMessageMetaRow struct {
	GUID              string
	RecordName        string
	PortalID          string
	ChatID            string
	TimestampMS       int64
	Sender            string
	IsFromMe          bool
	Service           string
	Deleted           bool
	ReplyTargetGUID   string
	TapbackTargetGUID string
	TapbackEmoji      string
	TapbackType       *uint32
	AttachmentGUIDs   []string

	RetryCount    int
	NextAttemptAt int64
	LastErrorKind string

	CreatedTS int64
	UpdatedTS int64
}

// bridgeAttachmentMetaRow is the in-memory shape of a bridge_attachment_meta row.
type bridgeAttachmentMetaRow struct {
	GUID          string
	MessageGUID   string
	MXCURI        string
	MIME          string
	SizeBytes     int64
	Width         int
	Height        int
	DurationMS    int
	IsLivePhoto   bool
	LivePairGUID  string
	RetryCount    int
	NextAttemptAt int64
	CreatedTS     int64
}

// ensureSchema creates the privacy-fork tables if they don't exist. Designed
// to run alongside cloudBackfillStore.ensureSchema during the transition: both
// schemas coexist so a half-migrated DB stays bootable.
func (s *bridgeMetaStore) ensureSchema(ctx context.Context) error {
	statements := []string{
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
		`CREATE INDEX IF NOT EXISTS bridge_message_meta_reply_idx
			ON bridge_message_meta (login_id, reply_target_guid)
			WHERE reply_target_guid IS NOT NULL AND reply_target_guid <> ''`,
		`CREATE INDEX IF NOT EXISTS bridge_message_meta_tapback_idx
			ON bridge_message_meta (login_id, tapback_target_guid)
			WHERE tapback_target_guid IS NOT NULL AND tapback_target_guid <> ''`,
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
		`CREATE INDEX IF NOT EXISTS bridge_attachment_meta_message_idx
			ON bridge_attachment_meta (login_id, message_guid)
			WHERE message_guid IS NOT NULL AND message_guid <> ''`,
		`CREATE INDEX IF NOT EXISTS bridge_attachment_meta_pair_idx
			ON bridge_attachment_meta (login_id, live_pair_guid)
			WHERE live_pair_guid IS NOT NULL AND live_pair_guid <> ''`,
	}
	for _, q := range statements {
		if _, err := s.db.Exec(ctx, q); err != nil {
			return fmt.Errorf("bridge_meta_store: %w", err)
		}
	}
	// Migrations: add missing columns to bridge_message_meta on already-
	// initialised databases (SQLite has no IF NOT EXISTS on ALTER).
	for _, col := range []struct{ name, def string }{
		{"record_name", "TEXT NOT NULL DEFAULT ''"},
	} {
		var exists int
		_ = s.db.QueryRow(ctx,
			`SELECT COUNT(*) FROM pragma_table_info('bridge_message_meta') WHERE name=$1`,
			col.name,
		).Scan(&exists)
		if exists == 0 {
			if _, err := s.db.Exec(ctx, fmt.Sprintf(
				`ALTER TABLE bridge_message_meta ADD COLUMN %s %s`, col.name, col.def,
			)); err != nil {
				return fmt.Errorf("add %s to bridge_message_meta: %w", col.name, err)
			}
		}
	}
	return nil
}

// upsertMessage writes (or updates) a single bridge_message_meta row. The
// `deleted` flag is sticky on update — once a row is tombstoned it stays
// tombstoned, matching cloud_message's behavior.
func (s *bridgeMetaStore) upsertMessage(ctx context.Context, row bridgeMessageMetaRow) error {
	nowMS := time.Now().UnixMilli()
	if row.CreatedTS == 0 {
		row.CreatedTS = nowMS
	}
	row.UpdatedTS = nowMS

	var attachmentGUIDsJSON sql.NullString
	if len(row.AttachmentGUIDs) > 0 {
		buf, err := json.Marshal(row.AttachmentGUIDs)
		if err != nil {
			return fmt.Errorf("marshal attachment_guids: %w", err)
		}
		attachmentGUIDsJSON = sql.NullString{String: string(buf), Valid: true}
	}

	var tapbackType sql.NullInt32
	if row.TapbackType != nil {
		tapbackType = sql.NullInt32{Int32: int32(*row.TapbackType), Valid: true}
	}

	var nextAttemptAt sql.NullInt64
	if row.NextAttemptAt > 0 {
		nextAttemptAt = sql.NullInt64{Int64: row.NextAttemptAt, Valid: true}
	}

	_, err := s.db.Exec(ctx, `
		INSERT INTO bridge_message_meta (
			login_id, guid, record_name, portal_id, chat_id, timestamp_ms, sender, is_from_me,
			service, deleted, reply_target_guid, tapback_target_guid, tapback_emoji,
			tapback_type, attachment_guids, retry_count, next_attempt_at,
			last_error_kind, created_ts, updated_ts
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
		)
		ON CONFLICT (login_id, guid) DO UPDATE SET
			record_name=CASE WHEN excluded.record_name <> '' THEN excluded.record_name ELSE bridge_message_meta.record_name END,
			portal_id=excluded.portal_id,
			chat_id=excluded.chat_id,
			timestamp_ms=excluded.timestamp_ms,
			sender=excluded.sender,
			is_from_me=excluded.is_from_me,
			service=excluded.service,
			deleted=CASE WHEN bridge_message_meta.deleted THEN bridge_message_meta.deleted ELSE excluded.deleted END,
			reply_target_guid=excluded.reply_target_guid,
			tapback_target_guid=excluded.tapback_target_guid,
			tapback_emoji=excluded.tapback_emoji,
			tapback_type=excluded.tapback_type,
			attachment_guids=excluded.attachment_guids,
			retry_count=excluded.retry_count,
			next_attempt_at=excluded.next_attempt_at,
			last_error_kind=excluded.last_error_kind,
			updated_ts=excluded.updated_ts
	`,
		s.loginID, row.GUID, row.RecordName,
		nullableString(stringPtrOrNil(row.PortalID)),
		nullableString(stringPtrOrNil(row.ChatID)), row.TimestampMS,
		nullableString(stringPtrOrNil(row.Sender)), row.IsFromMe,
		nullableString(stringPtrOrNil(row.Service)), row.Deleted,
		nullableString(stringPtrOrNil(row.ReplyTargetGUID)),
		nullableString(stringPtrOrNil(row.TapbackTargetGUID)),
		nullableString(stringPtrOrNil(row.TapbackEmoji)),
		tapbackType, attachmentGUIDsJSON, row.RetryCount, nextAttemptAt,
		nullableString(stringPtrOrNil(row.LastErrorKind)),
		row.CreatedTS, row.UpdatedTS,
	)
	if err != nil {
		return fmt.Errorf("upsert bridge_message_meta: %w", err)
	}
	return nil
}

// getMessage fetches a single bridge_message_meta row by GUID. Returns
// (nil, nil) when no row exists.
func (s *bridgeMetaStore) getMessage(ctx context.Context, guid string) (*bridgeMessageMetaRow, error) {
	var row bridgeMessageMetaRow
	var (
		portalID, chatID, sender, service, replyTarget, tapbackTarget, tapbackEmoji, errKind sql.NullString
		attachmentGUIDsJSON                                                                   sql.NullString
		tapbackType                                                                           sql.NullInt32
		nextAttemptAt                                                                         sql.NullInt64
	)
	err := s.db.QueryRow(ctx, `
		SELECT guid, portal_id, chat_id, timestamp_ms, sender, is_from_me, service,
		       deleted, reply_target_guid, tapback_target_guid, tapback_emoji, tapback_type,
		       attachment_guids, retry_count, next_attempt_at, last_error_kind,
		       created_ts, updated_ts
		FROM bridge_message_meta WHERE login_id=$1 AND guid=$2
	`, s.loginID, guid).Scan(
		&row.GUID, &portalID, &chatID, &row.TimestampMS, &sender, &row.IsFromMe, &service,
		&row.Deleted, &replyTarget, &tapbackTarget, &tapbackEmoji, &tapbackType,
		&attachmentGUIDsJSON, &row.RetryCount, &nextAttemptAt, &errKind,
		&row.CreatedTS, &row.UpdatedTS,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("getMessage: %w", err)
	}
	row.PortalID = portalID.String
	row.ChatID = chatID.String
	row.Sender = sender.String
	row.Service = service.String
	row.ReplyTargetGUID = replyTarget.String
	row.TapbackTargetGUID = tapbackTarget.String
	row.TapbackEmoji = tapbackEmoji.String
	row.LastErrorKind = errKind.String
	if tapbackType.Valid {
		t := uint32(tapbackType.Int32)
		row.TapbackType = &t
	}
	if nextAttemptAt.Valid {
		row.NextAttemptAt = nextAttemptAt.Int64
	}
	if attachmentGUIDsJSON.Valid && attachmentGUIDsJSON.String != "" {
		if err := json.Unmarshal([]byte(attachmentGUIDsJSON.String), &row.AttachmentGUIDs); err != nil {
			return nil, fmt.Errorf("unmarshal attachment_guids: %w", err)
		}
	}
	return &row, nil
}

// markMessageDeleted is a soft-delete that flips the tombstone bit without
// touching topology. Mirrors cloud_message's deleted=TRUE semantics.
func (s *bridgeMetaStore) markMessageDeleted(ctx context.Context, guid string) error {
	nowMS := time.Now().UnixMilli()
	_, err := s.db.Exec(ctx,
		`UPDATE bridge_message_meta SET deleted=TRUE, updated_ts=$3 WHERE login_id=$1 AND guid=$2`,
		s.loginID, guid, nowMS,
	)
	return err
}

// upsertMessageBatch writes many rows in a single transaction. Mirrors
// cloudBackfillStore.upsertMessageBatch — same prepared-statement pattern, same
// conflict resolution, just identifier-only columns.
func (s *bridgeMetaStore) upsertMessageBatch(ctx context.Context, rows []bridgeMessageMetaRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.RawDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO bridge_message_meta (
			login_id, guid, record_name, portal_id, chat_id, timestamp_ms, sender, is_from_me,
			service, deleted, reply_target_guid, tapback_target_guid, tapback_emoji,
			tapback_type, attachment_guids, retry_count, next_attempt_at,
			last_error_kind, created_ts, updated_ts
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (login_id, guid) DO UPDATE SET
			record_name=CASE WHEN excluded.record_name <> '' THEN excluded.record_name ELSE bridge_message_meta.record_name END,
			portal_id=excluded.portal_id,
			chat_id=excluded.chat_id,
			timestamp_ms=excluded.timestamp_ms,
			sender=excluded.sender,
			is_from_me=excluded.is_from_me,
			service=excluded.service,
			deleted=CASE WHEN bridge_message_meta.deleted THEN bridge_message_meta.deleted ELSE excluded.deleted END,
			reply_target_guid=excluded.reply_target_guid,
			tapback_target_guid=excluded.tapback_target_guid,
			tapback_emoji=excluded.tapback_emoji,
			tapback_type=excluded.tapback_type,
			attachment_guids=excluded.attachment_guids,
			updated_ts=excluded.updated_ts
	`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	defer stmt.Close()

	nowMS := time.Now().UnixMilli()
	for _, row := range rows {
		var attachmentGUIDsJSON sql.NullString
		if len(row.AttachmentGUIDs) > 0 {
			buf, marshalErr := json.Marshal(row.AttachmentGUIDs)
			if marshalErr != nil {
				return fmt.Errorf("marshal attachment_guids for %s: %w", row.GUID, marshalErr)
			}
			attachmentGUIDsJSON = sql.NullString{String: string(buf), Valid: true}
		}
		var tapbackType sql.NullInt32
		if row.TapbackType != nil {
			tapbackType = sql.NullInt32{Int32: int32(*row.TapbackType), Valid: true}
		}
		var nextAttemptAt sql.NullInt64
		if row.NextAttemptAt > 0 {
			nextAttemptAt = sql.NullInt64{Int64: row.NextAttemptAt, Valid: true}
		}
		if _, err = stmt.ExecContext(ctx,
			s.loginID, row.GUID, row.RecordName,
			nullableString(stringPtrOrNil(row.PortalID)),
			nullableString(stringPtrOrNil(row.ChatID)),
			row.TimestampMS,
			nullableString(stringPtrOrNil(row.Sender)),
			row.IsFromMe,
			nullableString(stringPtrOrNil(row.Service)),
			row.Deleted,
			nullableString(stringPtrOrNil(row.ReplyTargetGUID)),
			nullableString(stringPtrOrNil(row.TapbackTargetGUID)),
			nullableString(stringPtrOrNil(row.TapbackEmoji)),
			tapbackType, attachmentGUIDsJSON,
			row.RetryCount, nextAttemptAt,
			nullableString(stringPtrOrNil(row.LastErrorKind)),
			nowMS, nowMS,
		); err != nil {
			return fmt.Errorf("insert %s: %w", row.GUID, err)
		}
	}
	return tx.Commit()
}

// markMessageDeletedBatch sets deleted=TRUE for many GUIDs in a single
// statement. Mirrors cloudBackfillStore.deleteMessageBatch — soft-delete so a
// future re-sync can't resurrect tombstoned rows.
func (s *bridgeMetaStore) markMessageDeletedBatch(ctx context.Context, guids []string) error {
	if len(guids) == 0 {
		return nil
	}
	nowMS := time.Now().UnixMilli()
	const chunkSize = 500
	for i := 0; i < len(guids); i += chunkSize {
		end := i + chunkSize
		if end > len(guids) {
			end = len(guids)
		}
		chunk := guids[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)+2)
		args = append(args, s.loginID, nowMS)
		for j, guid := range chunk {
			placeholders[j] = fmt.Sprintf("$%d", j+3)
			args = append(args, guid)
		}
		query := fmt.Sprintf(
			`UPDATE bridge_message_meta SET deleted=TRUE, updated_ts=$2
			 WHERE login_id=$1 AND guid IN (%s)`,
			joinPlaceholders(placeholders),
		)
		if _, err := s.db.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("markMessageDeletedBatch: %w", err)
		}
	}
	return nil
}

// persistMessageUUID is the real-time APNs counterpart to upsertMessageBatch:
// stores GUID + topology so a future CloudKit replay (or a restart) dedups
// against an already-seen UUID. Uses INSERT OR IGNORE to be safe on repeat.
func (s *bridgeMetaStore) persistMessageUUID(ctx context.Context, uuid, portalID string, timestampMS int64, isFromMe bool) error {
	nowMS := time.Now().UnixMilli()
	_, err := s.db.Exec(ctx, `
		INSERT OR IGNORE INTO bridge_message_meta (
			login_id, guid, portal_id, timestamp_ms, is_from_me, deleted, retry_count, created_ts, updated_ts
		) VALUES ($1, $2, $3, $4, $5, FALSE, 0, $6, $7)
	`, s.loginID, uuid, nullableString(stringPtrOrNil(portalID)), timestampMS, isFromMe, nowMS, nowMS)
	return err
}

// persistTapbackUUID is persistMessageUUID + tapback_type, mirroring
// cloud_backfill_store.persistTapbackUUID's semantics so dedup-aware queries
// can distinguish a synthetic tapback row from a substantive message.
func (s *bridgeMetaStore) persistTapbackUUID(ctx context.Context, uuid, portalID string, timestampMS int64, isFromMe bool, tapbackType uint32) error {
	nowMS := time.Now().UnixMilli()
	_, err := s.db.Exec(ctx, `
		INSERT OR IGNORE INTO bridge_message_meta (
			login_id, guid, portal_id, timestamp_ms, is_from_me, deleted, tapback_type, retry_count, created_ts, updated_ts
		) VALUES ($1, $2, $3, $4, $5, FALSE, $6, 0, $7, $8)
	`, s.loginID, uuid, nullableString(stringPtrOrNil(portalID)), timestampMS, isFromMe, tapbackType, nowMS, nowMS)
	return err
}

// hasMessageUUID reports whether a GUID is known to bridge_message_meta for
// this login. Used for real-time echo detection: an APNs delivery whose UUID
// is already on disk is an echo of a previously-seen message and should not
// create a new portal or duplicate event.
//
// UPPER() on both sides: CloudKit GUIDs are lowercase, APNs UUIDs are
// uppercase, and incoming SMS constant_uuid values vary in case. A
// case-sensitive match would miss cross-path duplicates.
func (s *bridgeMetaStore) hasMessageUUID(ctx context.Context, uuid string) (bool, error) {
	var count int
	err := s.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM bridge_message_meta WHERE login_id=$1 AND UPPER(guid)=UPPER($2) LIMIT 1`,
		s.loginID, uuid,
	).Scan(&count)
	return count > 0, err
}

// hasPortalMessages reports whether the portal has at least one non-deleted
// CloudKit-imported message (record_name <> ''). Filters out the APNs stub
// rows (record_name='') the same way the cloud_backfill_store helper did.
func (s *bridgeMetaStore) hasPortalMessages(ctx context.Context, portalID string) (bool, error) {
	var count int
	err := s.db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM bridge_message_meta
		WHERE login_id=$1 AND portal_id=$2 AND deleted=FALSE AND record_name <> ''
	`, s.loginID, portalID).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// getNewestBackfillableMessageTimestamp returns the highest timestamp_ms
// among the portal's non-deleted CloudKit-imported messages.
//
// Note vs. cloud_backfill_store: that one supported a requireContentful
// flag that filtered text<>'' OR attachments_json<>''. We can't replicate
// that in bridge_message_meta because text/subject are intentionally
// absent. record_name<>'' is the closest proxy: APNs stubs (which have no
// content) have record_name=''; CloudKit-imported rows always carry it.
func (s *bridgeMetaStore) getNewestBackfillableMessageTimestamp(ctx context.Context, portalID string) (int64, error) {
	var ts sql.NullInt64
	err := s.db.QueryRow(ctx, `
		SELECT MAX(timestamp_ms)
		FROM bridge_message_meta
		WHERE login_id=$1 AND portal_id=$2 AND deleted=FALSE AND record_name <> ''
	`, s.loginID, portalID).Scan(&ts)
	if err != nil || !ts.Valid {
		return 0, err
	}
	return ts.Int64, nil
}

// isCloudBackfilledMessage reports whether a GUID was imported as a CloudKit
// backfill (record_name populated) vs an APNs-only real-time stub
// (record_name empty). Mirrors cloud_backfill_store.isCloudBackfilledMessage —
// same UPPER() casing, same record_name <> '' semantics.
func (s *bridgeMetaStore) isCloudBackfilledMessage(ctx context.Context, uuid string) (bool, error) {
	var exists bool
	err := s.db.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM bridge_message_meta
			WHERE login_id=$1 AND UPPER(guid)=UPPER($2) AND record_name <> ''
		)
	`, s.loginID, uuid).Scan(&exists)
	return exists, err
}

// hasMessageBatch checks existence of many GUIDs in a single query, returning
// the set that already exist. Used by ingestCloudMessages to distinguish
// "new" from "update" rows in the same way the cloud_backfill_store helper
// did for cloud_message.
func (s *bridgeMetaStore) hasMessageBatch(ctx context.Context, guids []string) (map[string]bool, error) {
	if len(guids) == 0 {
		return nil, nil
	}
	existing := make(map[string]bool, len(guids))
	const chunkSize = 500
	for i := 0; i < len(guids); i += chunkSize {
		end := i + chunkSize
		if end > len(guids) {
			end = len(guids)
		}
		chunk := guids[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)+1)
		args = append(args, s.loginID)
		for j, g := range chunk {
			placeholders[j] = fmt.Sprintf("$%d", j+2)
			args = append(args, g)
		}
		query := fmt.Sprintf(
			`SELECT guid FROM bridge_message_meta WHERE login_id=$1 AND guid IN (%s)`,
			joinPlaceholders(placeholders),
		)
		rows, err := s.db.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var guid string
			if err := rows.Scan(&guid); err != nil {
				rows.Close()
				return nil, err
			}
			existing[guid] = true
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return existing, nil
}

func joinPlaceholders(ps []string) string {
	out := ""
	for i, p := range ps {
		if i > 0 {
			out += ","
		}
		out += p
	}
	return out
}

// upsertAttachment writes (or updates) a single bridge_attachment_meta row.
// The mxc_uri may be empty on insert (pre-upload) and filled in by a later
// update once the Matrix media upload completes — this is the resumability
// signal the spec relies on.
func (s *bridgeMetaStore) upsertAttachment(ctx context.Context, row bridgeAttachmentMetaRow) error {
	if row.CreatedTS == 0 {
		row.CreatedTS = time.Now().UnixMilli()
	}
	var nextAttemptAt sql.NullInt64
	if row.NextAttemptAt > 0 {
		nextAttemptAt = sql.NullInt64{Int64: row.NextAttemptAt, Valid: true}
	}
	_, err := s.db.Exec(ctx, `
		INSERT INTO bridge_attachment_meta (
			login_id, guid, message_guid, mxc_uri, mime, size_bytes, width, height,
			duration_ms, is_live_photo, live_pair_guid, retry_count, next_attempt_at, created_ts
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)
		ON CONFLICT (login_id, guid) DO UPDATE SET
			message_guid=excluded.message_guid,
			mxc_uri=CASE WHEN excluded.mxc_uri <> '' THEN excluded.mxc_uri ELSE bridge_attachment_meta.mxc_uri END,
			mime=excluded.mime,
			size_bytes=excluded.size_bytes,
			width=excluded.width,
			height=excluded.height,
			duration_ms=excluded.duration_ms,
			is_live_photo=excluded.is_live_photo,
			live_pair_guid=excluded.live_pair_guid,
			retry_count=excluded.retry_count,
			next_attempt_at=excluded.next_attempt_at
	`,
		s.loginID, row.GUID, nullableString(stringPtrOrNil(row.MessageGUID)),
		row.MXCURI, nullableString(stringPtrOrNil(row.MIME)),
		row.SizeBytes, row.Width, row.Height, row.DurationMS,
		row.IsLivePhoto, nullableString(stringPtrOrNil(row.LivePairGUID)),
		row.RetryCount, nextAttemptAt, row.CreatedTS,
	)
	if err != nil {
		return fmt.Errorf("upsert bridge_attachment_meta: %w", err)
	}
	return nil
}

// getAttachment fetches a single attachment metadata row. (nil, nil) when missing.
func (s *bridgeMetaStore) getAttachment(ctx context.Context, guid string) (*bridgeAttachmentMetaRow, error) {
	var row bridgeAttachmentMetaRow
	var (
		messageGUID, mime, mxcURI, livePair sql.NullString
		nextAttemptAt                       sql.NullInt64
	)
	err := s.db.QueryRow(ctx, `
		SELECT guid, message_guid, mxc_uri, mime, size_bytes, width, height, duration_ms,
		       is_live_photo, live_pair_guid, retry_count, next_attempt_at, created_ts
		FROM bridge_attachment_meta WHERE login_id=$1 AND guid=$2
	`, s.loginID, guid).Scan(
		&row.GUID, &messageGUID, &mxcURI, &mime, &row.SizeBytes, &row.Width, &row.Height,
		&row.DurationMS, &row.IsLivePhoto, &livePair, &row.RetryCount, &nextAttemptAt, &row.CreatedTS,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("getAttachment: %w", err)
	}
	row.MessageGUID = messageGUID.String
	row.MXCURI = mxcURI.String
	row.MIME = mime.String
	row.LivePairGUID = livePair.String
	if nextAttemptAt.Valid {
		row.NextAttemptAt = nextAttemptAt.Int64
	}
	return &row, nil
}

// stringPtrOrNil returns nil for an empty string, &s otherwise. Convenience for
// passing nullable strings into nullableString() without inline if-statements.
func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
