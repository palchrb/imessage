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
// to Matrix without consulting cloud_message. Text and Subject live here in
// RAM only — they never get persisted by anything that touches this struct.
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
	DateReadMS        int64
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
	StartedAt        time.Time
	Portals          int
	FlushedPortals   int
	Messages         int
	ApproxTextBytes  int64
	OldestMessageMS  int64
	NewestMessageMS  int64
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
