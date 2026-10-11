// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build race

package link

// raceEnabled: sync.Pool drops items at random under -race.
const raceEnabled = true
