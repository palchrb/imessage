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
type bridgeMessageMetaRow struct {
	GUID              string
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
			login_id, guid, portal_id, chat_id, timestamp_ms, sender, is_from_me,
			service, deleted, reply_target_guid, tapback_target_guid, tapback_emoji,
			tapback_type, attachment_guids, retry_count, next_attempt_at,
			last_error_kind, created_ts, updated_ts
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
		)
		ON CONFLICT (login_id, guid) DO UPDATE SET
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
		s.loginID, row.GUID, nullableString(stringPtrOrNil(row.PortalID)),
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
