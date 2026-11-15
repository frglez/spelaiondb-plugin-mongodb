# spelaiondb-plugin-mongodb

MongoDB for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), served as a plugin: a
program SpelaionDB starts and talks to over its standard input and output (its ADR-041). It
also serves FerretDB, which speaks MongoDB's protocol.

SpelaionDB's core reads what you type in the editor (a subset of the MongoDB shell's method
calls) and sends this plugin one command document per statement (its ADR-056); the plugin runs
it and returns the documents as rows and columns. Read-only mode and agents are not offered on
MongoDB yet.

- **Tested against:** MongoDB 8.0 and FerretDB 2 (`testing/compose.yaml`).
- **Not tested here:** MongoDB Atlas, Amazon DocumentDB and Azure Cosmos DB (paid services with
  no local image). The plugin connects only to the address SpelaionDB gives it; any DNS SRV
  discovery is the core's. It accepts `authSource`, `authMechanism` and `replicaSet` settings.
  `replicaSet` checks the expected set without discovering other members. Verified TLS through
  a tunnel is still unavailable: MongoDB's Rust driver checks the socket's host name, which
  is the tunnel's loopback address. The plugin refuses that mode instead of weakening TLS.

Protocol: [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
version 1.1, experimental until SpelaionDB 1.0. Rules for contributors and agents: `AGENTS.md`.
License: Apache-2.0.
