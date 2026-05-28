// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2024 Tulir Asokan, Ludvig Rhodin
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package connector

import (
	"fmt"
	"time"

	"maunium.net/go/mautrix/bridgev2/commands"
)

// cmdPrivacySessionStats inspects the RAM-staging session that the privacy
// fork uses during CloudKit sync. Useful for verifying that the session is
// being populated as messages stream in, and for sizing the eventual
// max_staging_ram_mb safety net.
var cmdPrivacySessionStats = &commands.FullHandler{
	Name:          "privacy-session-stats",
	Aliases:       []string{"privacy-stats"},
	Func:          fnPrivacySessionStats,
	RequiresLogin: true,
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionGeneral,
		Description: "Show the privacy-fork RAM-staging session size: portals, messages, approximate cleartext bytes held in RAM.",
	},
}

func fnPrivacySessionStats(ce *commands.Event) {
	login := ce.User.GetDefaultLogin()
	if login == nil {
		ce.Reply("Not logged in.")
		return
	}
	client, ok := login.Client.(*IMClient)
	if !ok || client == nil {
		ce.Reply("Bridge client not available.")
		return
	}

	client.cloudSessionLock.RLock()
	session := client.cloudSession
	client.cloudSessionLock.RUnlock()

	if session == nil {
		ce.Reply("No active CloudKit sync — RAM staging session is not running.")
		return
	}
	stats := session.Stats()
	age := time.Since(stats.StartedAt).Truncate(time.Second)

	ce.Reply("**Privacy-fork RAM staging session**\n"+
		"Started:        %s ago\n"+
		"Portals:        %d (%d flushed)\n"+
		"Messages:       %d\n"+
		"Cleartext:      ~%.2f MiB\n"+
		"Oldest message: %s\n"+
		"Newest message: %s\n",
		age,
		stats.Portals, stats.FlushedPortals,
		stats.Messages,
		float64(stats.ApproxTextBytes)/(1024*1024),
		formatMessageTS(stats.OldestMessageMS),
		formatMessageTS(stats.NewestMessageMS),
	)
}

func formatMessageTS(ms int64) string {
	if ms == 0 {
		return "—"
	}
	return fmt.Sprintf("%s UTC", time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05"))
}
