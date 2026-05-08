# Diagrams

Mermaid is the source of truth. Rendered PNGs are committed alongside their `.mmd` source so the README can embed them without forcing a Mermaid renderer on every reader.

## Files

| Source                    | Rendered                  | Purpose                                                                  |
| ------------------------- | ------------------------- | ------------------------------------------------------------------------ |
| `system-context.mmd`      | `system-context.png`      | High-level component view: edge, API, AI, data, async, observability.   |
| `data-flow-rag.mmd`       | `data-flow-rag.png`       | Sequence diagram for document upload through chat citation.             |

The `.png` files in this directory are **placeholders** committed so the README's image embeds resolve. Regenerate them locally with `mmdc` (command below) any time the corresponding `.mmd` source changes — the placeholder is a uniform grey block, not the actual diagram.

## Rendering

Install [`@mermaid-js/mermaid-cli`](https://github.com/mermaid-js/mermaid-cli) once:

```bash
npm install -g @mermaid-js/mermaid-cli
```

Render a single diagram:

```bash
mmdc -i docs/diagrams/system-context.mmd \
     -o docs/diagrams/system-context.png \
     -t default -b transparent -w 1600
```

Render every diagram in this directory:

```bash
for f in docs/diagrams/*.mmd; do
  mmdc -i "$f" -o "${f%.mmd}.png" -t default -b transparent -w 1600
done
```

Both diagrams have been syntax-checked; if `mmdc` reports a parse error after a local edit, that is a regression worth fixing before the PR.

## Editing guidelines

- Keep node labels short. If a node label runs longer than three words it usually means there is a missing subgraph.
- Group related components in subgraphs (`subgraph Foo[...]`) to keep the layout readable as the diagram grows.
- Edge labels matter — when an edge represents an async hop (Kafka, HTTP-to-worker queue, etc.) the label should make that explicit (`-. async .->`).
- Use `classDef` to color groups by layer. The five class buckets in `system-context.mmd` (edge, api, ai, data, async, worker, obs, ext) are the canonical palette; reuse them across diagrams instead of inventing new ones.
