// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2024 Tulir Asokan, Ludvig Rhodin
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

import (
	"sync"
	"time"
)

// cloudSyncSession is the per-sync RAM staging layer the privacy-fork spec
// calls for: every WrappedCloudSyncMessage that arrives from CloudKit is
// accumulated in-memory with its text intact, and dropped after the sync's
// flush phase completes. The cleartext never reaches bridge_message_meta or
// (in the eventual end state) cloud_message — only GUIDs/topology hit disk.
//
// This file ships only the data structures and lifecycle plumbing. Readers
// (the FetchMessages path) wire up in a follow-up commit; for now the session
// is populated alongside the existing cloud_message write and inspected via
// the `!im privacy session-stats` debug command.
//
// Concurrency: a single sync run owns the session, but ingestCloudMessages
// can be called from multiple goroutines if the per-zone fetchers ever go
// parallel. All session mutations go through Append/MarkPortalFlushed under
// the mutex.
type cloudSyncSession struct {
	mu        sync.RWMutex
	portals   map[string]*portalSessionState
	startedAt time.Time
}

// portalSessionState is the per-portal accumulator. Messages are appended
// unordered as pages arrive; the flusher sorts by timestamp at flush time.
type portalSessionState struct {
	Messages []sessionMessage
	// Flushed marks the portal as fully drained to Matrix. Once every portal
	// is flushed, the parent session can be dropped.
	Flushed bool
}

// sessionMessage holds the fields needed to render a single CloudKit message
// to Matrix without consulting cloud_message. Text, Subject and the rich
// attachment-JSON live here in RAM only — they never get persisted by
// anything that touches this struct.
//
// AttachmentsJSON mirrors cloudMessageRow.AttachmentsJSON exactly (a
// serialised []cloudAttachmentRow) so downstream conversion code can stay
// schema-stable across the cloud_message-vs-session source switch.
type sessionMessage struct {
	GUID              string
	RecordName        string
	PortalID          string
	ChatID            string
	TimestampMS       int64
	Sender            string
	IsFromMe          bool
	Service           string
	Text              string // RAM-only — never persisted
	Subject           string // RAM-only
	HasBody           bool
	TapbackType       *uint32
	TapbackTargetGUID string
	TapbackEmoji      string
	AttachmentGUIDs   []string
	AttachmentsJSON   string // RAM-only — full []cloudAttachmentRow JSON
	DateReadMS        int64
}

// toCloudMessageRow shapes a sessionMessage into the cloudMessageRow the
// existing render path expects. Lets the flush phase plug session data into
// cloudRowsToBackfillMessages with no downstream changes.
func (m sessionMessage) toCloudMessageRow() cloudMessageRow {
	return cloudMessageRow{
		GUID:              m.GUID,
		RecordName:        m.RecordName,
		CloudChatID:       m.ChatID,
		PortalID:          m.PortalID,
		TimestampMS:       m.TimestampMS,
		Sender:            m.Sender,
		IsFromMe:          m.IsFromMe,
		Text:              m.Text,
		Subject:           m.Subject,
		Service:           m.Service,
		Deleted:           false, // session never holds tombstones
		TapbackType:       m.TapbackType,
		TapbackTargetGUID: m.TapbackTargetGUID,
		TapbackEmoji:      m.TapbackEmoji,
		AttachmentsJSON:   m.AttachmentsJSON,
		DateReadMS:        m.DateReadMS,
		HasBody:           m.HasBody,
	}
}

func newCloudSyncSession() *cloudSyncSession {
	return &cloudSyncSession{
		portals:   make(map[string]*portalSessionState),
		startedAt: time.Now(),
	}
}

// Append records a single message into the per-portal accumulator. portalID
// must already be resolved by the caller.
func (s *cloudSyncSession) Append(portalID string, m sessionMessage) {
	if s == nil || portalID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.portals[portalID]
	if !ok {
		state = &portalSessionState{}
		s.portals[portalID] = state
	}
	state.Messages = append(state.Messages, m)
}

// MarkPortalFlushed flags a portal as drained. AllFlushed can then decide to
// drop the session.
func (s *cloudSyncSession) MarkPortalFlushed(portalID string) {
	if s == nil || portalID == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.portals[portalID]; ok {
		state.Flushed = true
	}
}

// AllFlushed reports whether every accumulated portal has been flushed to
// Matrix. Used by the lifecycle owner to decide when to drop the session.
func (s *cloudSyncSession) AllFlushed() bool {
	if s == nil {
		return true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.portals) == 0 {
		return true
	}
	for _, state := range s.portals {
		if !state.Flushed {
			return false
		}
	}
	return true
}

// Stats returns a snapshot of session shape — total portals, total messages,
// approximate text bytes — for the privacy-stats debug command and for the
// RAM-budget tracking the brief calls for.
type cloudSyncSessionStats struct {
	StartedAt       time.Time
	Portals         int
	FlushedPortals  int
	Messages        int
	ApproxTextBytes int64
	OldestMessageMS int64
	NewestMessageMS int64
}

// PortalIDs returns the list of portals currently in the session. Used by the
// flush phase to iterate without holding the session lock.
func (s *cloudSyncSession) PortalIDs() []string {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.portals))
	for id := range s.portals {
		out = append(out, id)
	}
	return out
}

// GetSortedMessages returns the session's messages for a portal in
// chronological (ascending timestamp) order. Returns nil if the portal has
// no session data.
func (s *cloudSyncSession) GetSortedMessages(portalID string) []sessionMessage {
	if s == nil || portalID == "" {
		return nil
	}
	s.mu.RLock()
	state, ok := s.portals[portalID]
	if !ok {
		s.mu.RUnlock()
		return nil
	}
	out := make([]sessionMessage, len(state.Messages))
	copy(out, state.Messages)
	s.mu.RUnlock()
	sortSessionMessagesByTS(out)
	return out
}

func sortSessionMessagesByTS(msgs []sessionMessage) {
	// Insertion sort: pages arrive nearly-sorted, so this is O(n) on the
	// common path and avoids importing sort just for one call site.
	for i := 1; i < len(msgs); i++ {
		j := i
		for j > 0 && msgs[j-1].TimestampMS > msgs[j].TimestampMS {
			msgs[j-1], msgs[j] = msgs[j], msgs[j-1]
			j--
		}
	}
}

func (s *cloudSyncSession) Stats() cloudSyncSessionStats {
	stats := cloudSyncSessionStats{}
	if s == nil {
		return stats
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	stats.StartedAt = s.startedAt
	stats.Portals = len(s.portals)
	for _, state := range s.portals {
		if state.Flushed {
			stats.FlushedPortals++
		}
		stats.Messages += len(state.Messages)
		for _, msg := range state.Messages {
			stats.ApproxTextBytes += int64(len(msg.Text)) + int64(len(msg.Subject))
			if stats.OldestMessageMS == 0 || msg.TimestampMS < stats.OldestMessageMS {
				stats.OldestMessageMS = msg.TimestampMS
			}
			if msg.TimestampMS > stats.NewestMessageMS {
				stats.NewestMessageMS = msg.TimestampMS
			}
		}
	}
	return stats
}
