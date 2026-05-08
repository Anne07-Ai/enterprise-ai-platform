---
name: Bug report
about: A defect in something that already works.
title: "fix: <short description>"
labels: [bug, needs-triage]
assignees: []
---

## Summary

What happened, in one sentence.

## Expected vs actual

Expected: what should have happened.

Actual: what did happen.

## Reproduction

Minimal steps to reproduce. Include exact commands, request payloads, or screenshots if relevant.

```bash
# steps
```

## Environment

- Component: api / web / ingestion-worker / embedding-worker / agent-runtime / infra
- Commit SHA: `<sha>`
- OS / Docker version:
- Compose stack state at time of bug: `make ps` output

## Logs and traces

Relevant log lines and trace IDs. Truncate to the smallest set that demonstrates the problem.

```text
<logs>
```

## Severity

- [ ] Production-impacting (data loss, security, or availability)
- [ ] Cross-tenant impact suspected
- [ ] Local development annoyance only

## Notes

Anything you've already tried, suspected root cause, or related issues.
