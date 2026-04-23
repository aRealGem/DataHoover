## Kanban

Lightweight task tracker for DataHoover. Three columns, three files:

- `backlog.md` — triaged-but-not-yet-started work
- `wip.md` — actively in progress (one owner per card, ideally)
- `done.md` — recently completed (prune periodically into `done-archive.md`)

### Card format

```
### [AREA] Short title
- **Added:** YYYY-MM-DD  **Owner:** name/agent  **PR:** #123 (if any)
- **Problem:** one sentence.
- **Acceptance:** bullet list of observable outcomes.
- **Notes:** links, error excerpts, follow-ups.
```

Keep cards atomic enough that a single PR closes one card.

### Movement

- New work lands in `backlog.md`. When pulled, cut-paste to `wip.md`.
- On merge, cut-paste to `done.md` and append `**Closed:** YYYY-MM-DD  **PR:** <link>`.
- Do not rewrite history; cards keep the original `Added` date.
