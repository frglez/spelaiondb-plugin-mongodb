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
  no local image). `mongodb+srv` addresses are not supported: the plugin connects only to the
  address SpelaionDB gives it.

Protocol: [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
version 1.1, experimental until SpelaionDB 1.0. Rules for contributors and agents: `AGENTS.md`.
License: Apache-2.0.
