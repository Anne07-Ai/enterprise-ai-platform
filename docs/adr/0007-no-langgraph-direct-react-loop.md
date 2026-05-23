# ADR-0007: No LangGraph for the agent loop

## Status

Accepted.

## Context

The Phase 4 chat agent needs to call tools mid-conversation and loop
until the model emits an end-of-turn signal. Three classes of solution
exist for this in 2026:

1. **LangGraph** (or LangChain agents) — declare a `StateGraph` with
   nodes for the model, the tools, and conditional edges that route
   based on whether the model wants to call a tool.
2. **Pre-built agent frameworks** with built-in ReAct loops
   (CrewAI, AutoGen, smol-agents, etc.). All add their own state
   model, prompt conventions, and observability layer.
3. **Direct implementation** against the LLM provider's SDK. For
   Anthropic this means iterating over the `messages.create`
   response, branching on `stop_reason`, and looping until
   `stop_reason == "end_turn"`.

LangGraph in particular is the default option in tutorials and
starter templates. It offers persistent state, conditional routing,
parallel branches, human-in-the-loop interrupts, and a checkpoint
system for resumable runs.

Our agent does none of those things. It has:

- Two tools (`search_documents`, `get_document`)
- One linear control flow: call model → if tool_use, run tool, loop
  → if end_turn, return
- A hard ceiling on iterations to bound cost
- No persistence, no checkpoints, no branching, no human approvals

The Anthropic SDK natively returns tool_use blocks; mapping them to
Python functions is straightforward.

## Decision

Implement the agent loop directly against `anthropic.AsyncAnthropic`.
No LangGraph, no LangChain, no other framework.

The loop in `apps/api/app/modules/chat/agent.py` is ~80 lines:

```python
while iterations < MAX_ITERATIONS:
    resp = await client.messages.create(...)
    messages.append({"role": "assistant", "content": resp.content})
    if resp.stop_reason == "end_turn":
        return final_text
    for block in resp.content:
        if block.type != "tool_use":
            continue
        result = await _dispatch_tool(...)
        tool_results.append({...})
    messages.append({"role": "user", "content": tool_results})
```

We DO install `langgraph` and `langchain-anthropic` as dependencies
so a future migration is a code change, not a dependency change.
If we later need persistent state, conditional routing, or human
interrupts, swapping to LangGraph is a contained refactor — the
tools and schemas are framework-agnostic.

## Alternatives considered

**LangGraph StateGraph.** Rejected for the current scope. The
benefits (persistence, routing, checkpoints) are unused. The costs
are real: four layers of abstraction (StateGraph, nodes, edges,
state schema) around what is fundamentally a `while` loop; an extra
prompt-templating layer between us and the model; debugging requires
understanding LangGraph internals on top of Anthropic's API. For
an interview review, "I implemented the ReAct loop directly" is
more credible than "I used LangGraph because the tutorial did."

**CrewAI / AutoGen / smol-agents.** Same rejection plus an extra
one: these frameworks bring their own opinions about multi-agent
orchestration that we don't need.

**OpenAI Assistants API style "server-managed thread".** Rejected
because Anthropic's SDK does not expose this; the equivalent would
be us implementing thread storage server-side, which is exactly
what we want to avoid in this phase. Client-supplied history
(`request.history` in the chat schema) is simpler and stateless.

## Consequences

Positive: the loop is debuggable end-to-end with a Python debugger.
Cost is bounded by `MAX_ITERATIONS=6` so a misbehaving model cannot
run up the bill. The same `agent.run()` function can be reused from
HTTP, from CLI scripts (see `app/scripts/test_agent.py`), or from
worker code with no framework setup. Streaming (Phase 4c) reuses
the same control flow with `client.messages.stream(...)`.

Negative: we own the loop. If Anthropic changes the tool-use
protocol shape (it has shifted twice in the last 18 months), we
adapt. A framework would absorb that change for us. Acceptable
trade for the current complexity level; revisit if our tool count
exceeds ~5 or we need branching control flow.

The `langgraph` and `langchain-anthropic` dependencies remain in
`pyproject.toml` as a hedge against future migration. They cost
~30 transitive packages at install time and zero runtime overhead
because we don't import them.
