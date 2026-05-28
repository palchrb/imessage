// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2024 Tulir Asokan, Ludvig Rhodin
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

package main

import (
	"flag"
	"fmt"
	"os"

	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
)

// runMigrate is the entrypoint for `mautrix-imessage migrate ...`. Today it
// only validates flags and prints a "not yet implemented" notice. The real
// migration lands later — see the privacy-fork spec for the full plan
// (drop cloud_message + cloud_attachment_cache, repopulate the new
// identifier-only tables from existing topology).
//
// We wire this up as a stub now so the final CLI surface
// (`mautrix-imessage migrate --to-privacy-fork`) is stable from day one and
// install scripts / docs can reference it without churn.
func runMigrate(_ *mxmain.BridgeMain) {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	toPrivacyFork := fs.Bool("to-privacy-fork", false,
		"Migrate an upstream/Jason cleartext bridge.db to this fork's identifier-only schema (one-way, irreversible).")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "Usage: mautrix-imessage migrate --to-privacy-fork")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Migrates an existing bridge database from upstream (lrhodin/jasonlaguidice)")
		fmt.Fprintln(fs.Output(), "to this fork's identifier-only schema. Drops cleartext message + attachment")
		fmt.Fprintln(fs.Output(), "tables and rebuilds topology in bridge_message_meta + bridge_attachment_meta.")
		fmt.Fprintln(fs.Output(), "")
		fs.PrintDefaults()
	}

	// fs.Parse exits on -h / parse error via ExitOnError.
	_ = fs.Parse(os.Args[1:])

	if !*toPrivacyFork {
		fs.Usage()
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "error: --to-privacy-fork is required (no other migration targets exist)")
		os.Exit(2)
	}

	fmt.Fprintln(os.Stderr, "migrate --to-privacy-fork: not yet implemented")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "This subcommand is a stub. The full conversion will:")
	fmt.Fprintln(os.Stderr, "  1. Back-fill bridge_message_meta from cloud_message (topology only — text dropped)")
	fmt.Fprintln(os.Stderr, "  2. Back-fill bridge_attachment_meta from existing attachment rows")
	fmt.Fprintln(os.Stderr, "  3. Hard-delete cloud_message + cloud_attachment_cache")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Until the migration ships, do NOT run this against a production bridge.db.")
	os.Exit(1)
}
