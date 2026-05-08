# Pull request

## What and why

A one- or two-paragraph summary. Lead with the user-visible change or the operational outcome, then the reasoning. If this implements an ADR, link it.

## Scope

- [ ] One concern only (refactor, feature, bug fix, infra change — pick one)
- [ ] Touches only the modules listed below

Modules touched:

- `<module>` — `<one-line summary>`

## How it was tested

How you convinced yourself this works. Specific commands, screenshots of the dashboard, sample requests, eval scores — whichever is appropriate.

```bash
# example
make up && make healthcheck
```

## ADR / docs

- [ ] Linked an existing ADR if this implements a prior decision.
- [ ] Added a new ADR if this introduces a new load-bearing decision.
- [ ] README / ARCHITECTURE updated if module boundaries or data flow changed.
- [ ] Runbook added under `docs/runbooks/` if this introduces a new operational concern.

## Risk

- [ ] Reversible by `git revert` alone (no data migration, no irreversible infra change).
- [ ] Migration is forward-only; rollback plan documented in PR body.

## Checklist

- [ ] Conventional Commit title (`feat:`, `fix:`, `docs:`, `infra:`, `chore:`, `refactor:`, `test:`).
- [ ] Pre-commit passes (`pre-commit run --all-files`).
- [ ] CI green.
- [ ] No secrets, credentials, or `.env` content in the diff.

## Linked issues

Closes #
