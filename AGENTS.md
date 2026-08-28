# AGENTS

## Project Overview

DBStress is an Oracle workload lab with:

- a Node.js/Express backend in `server/`
- a React frontend in `client/`
- a separate FastAPI GC benchmark app in `gc_benchmark/gc_benchmark/`

Main application entry points:

- `server/index.js`
- `client/src/App.js`

## Working Style

- Prefer implementing requested changes directly instead of only proposing them.
- Preserve existing Oracle-focused workflows and UI patterns unless the user asks for a redesign.
- Use `rg` for fast file and text search.
- Use `apply_patch` for manual file edits.
- Avoid reverting user changes unless the user explicitly asks for that.

## Validation

- For backend JavaScript changes, run `node --check` on edited server files when practical.
- For frontend changes, run `node --check` on edited client files when practical.
- Run `npm run build` from `client/` after UI changes when practical.

## Git Workflow

- For every change the user wants to keep, commit it to local git.
- After committing locally, push the change to the remote branch as well.
- If a commit message is not specified by the user, write a clear message describing the actual change.
- If pushing or committing would be risky or blocked, explain the reason clearly.

## Notes

- There was no existing `agent.md` or `AGENTS.md` in this repository before this file was added.
