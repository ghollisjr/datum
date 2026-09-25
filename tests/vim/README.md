# Vim Client Test Plan (Stub)

This directory is reserved for future Vim/Neovim client tests.

## What a Vim client would need

The Datum envelope protocol (`##DATUM:<type>:<payload>##`) is editor-agnostic.
A Vim client plugin would need to:

1. **Parse envelope lines** from the terminal job output, stripping them from
   visible display (similar to `sql-datum--preoutput-filter` in Emacs).

2. **Handle envelope types:**
   - `dialect` — set buffer-local SQL dialect for syntax highlighting
   - `meta` — update statusline with database/user/version info
   - `introspect` / `introspect+` — populate completion lists (tables, columns,
     routines, schemas, databases)
   - `definition` — display DDL in a split/float window
   - `running-text` — display running queries in a dedicated buffer
   - `result-file` — notify user of export completion
   - `info` / `warn` / `error` — display messages

3. **Provide completion** using the introspection data (omnifunc or LSP-style).

## Tests to add

When a Vim client is written, add tests here that:

- Parse synthetic envelope strings and verify state updates
- Verify envelope lines are stripped from visible output
- Test chunked `introspect+` appending
- Test partial line buffering across output callbacks
- Test completion candidate generation from introspection state

Tests can use Vim's `-c` batch execution or a test framework like Vader.vim
or Themis.
