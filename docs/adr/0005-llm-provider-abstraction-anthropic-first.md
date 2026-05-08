# ADR-0005: LLM provider abstraction with Anthropic as the default

## Status

Accepted — 2026-05-08

## Context

The platform calls hosted LLM providers for chat completion, tool use, and (in places) embeddings. The provider landscape changes monthly: capability gaps open and close, prices shift, regional availability and data-residency commitments differ, and individual providers have outages on the order of hours. Hard-coding any one provider's SDK throughout the application would couple our codebase to a moving target and force a multi-week rewrite the first time we needed to fail over, switch for cost reasons, or support a customer with a non-negotiable data-residency requirement.

At the same time, an over-engineered "universal provider abstraction" is its own anti-pattern. Provider features that are genuinely incompatible (Anthropic's tool-use schema versus OpenAI's, Bedrock's authentication versus Anthropic's API keys, streaming chunk shapes that mean different things) cannot be hidden behind a thin interface without either lowest-common-denominator-ing the interface or leaking the differences anyway. The right abstraction is one that captures the shape we actually need and pushes provider-specific quirks to the implementation behind it.

Anthropic is the default because Claude is currently best-in-class for the workloads this platform targets — long-context retrieval-grounded chat and tool-using agents — and because the Anthropic Messages API's tool-use shape is closest to the agent runtime's mental model. We are not making a forever bet, we are making a current bet with a clean exit.

## Decision

A thin provider interface lives in `apps/api/app/ai/providers/`. The interface defines exactly the operations the rest of the application uses:

- `complete_streaming(messages, tools, model, ...) -> AsyncIterator[ProviderEvent]`
- `count_tokens(messages, model) -> int`
- `embed_batch(texts, model) -> list[Vector]` (only on providers that expose embeddings)

`ProviderEvent` is a small sealed union: `TokenDelta`, `ToolUseStart`, `ToolUseInput`, `ToolUseStop`, `Stop`, `Error`. Each provider implementation translates its own SSE/streaming format into these events. Anthropic ships first, OpenAI is the second implementation, Bedrock and Azure OpenAI are pluggable behind the same interface.

Provider selection is configured per workspace with a fallback chain. The LLM gateway worker handles routing: it dispatches to the primary provider, watches for provider-class errors (rate limit, 5xx, timeout, content filter), and falls back to the next in the chain. Per-provider circuit breakers keep us from hammering a degraded upstream.

Provider-specific quirks live behind the interface and never leak:

- **Streaming format**: Anthropic emits `message_start`, `content_block_start`, `content_block_delta`, etc.; OpenAI emits `data: {...}` chunks; Bedrock emits its own framing. All three are translated into the same `ProviderEvent` stream at the implementation boundary.
- **Tool-call shape**: Anthropic's `tool_use` content blocks and OpenAI's `tool_calls` array are translated to a normalized `ToolUseStart`/`ToolUseInput`/`ToolUseStop` sequence.
- **Token counting**: each implementation calls the provider's own counter (Anthropic's `messages/count_tokens`, OpenAI's tiktoken). The interface returns an integer; callers do not know which library was used.
- **Authentication**: API keys for Anthropic/OpenAI, IAM-based for Bedrock. The provider implementation owns its own credential lifecycle.

## Alternatives Considered

**Hard-code the Anthropic SDK throughout.** Rejected because the cost of a future migration is paid in every call site at once. Also rejected because per-workspace provider selection — a real product requirement for cost and data-residency — would require either a parallel code path or the abstraction we are committing to anyway.

**Use LangChain as the provider abstraction.** Rejected because LangChain is a much bigger commitment than the abstraction we need. It pulls in concepts (chains, runnables, callbacks) that overlap with our agent runtime in incompatible ways, and its release cadence has historically broken consumers. We want the boundary, not the framework.

**Use LiteLLM as a unifying proxy.** Considered seriously. LiteLLM normalizes provider APIs to an OpenAI-shaped interface and is an explicit answer to this problem. Rejected because (a) "OpenAI-shaped" is the lowest-common-denominator interface and we want Anthropic-shaped tool use as the canonical form because it maps to our agent runtime more cleanly, (b) running a proxy adds a hop and a service to operate, and (c) the abstraction we want is small enough to write directly. We may revisit if the per-provider implementation becomes a maintenance burden.

**One provider per workload (e.g., Voyage for embeddings, Anthropic for chat, OpenAI for moderation).** Not actually rejected — this is what we do. The decision here is about how providers are abstracted, not about which one is used for each call.

**Defer the abstraction and add it when needed.** Rejected because the marginal cost of adding the interface now is low and the marginal cost of refactoring every call site later is high. This is the YAGNI exception: when an abstraction is small and inevitable, build it now.

## Consequences

Positive: provider migration is a bounded change confined to `apps/api/app/ai/providers/`. Per-workspace provider routing and fallback are first-class. Mocking the LLM in tests is a one-class fake that yields a deterministic `ProviderEvent` stream. Cost optimization (route cheap workloads to a cheap provider, route expensive workloads to a strong provider) is a configuration change, not a code change. Regulatory flexibility for data residency (route an EU customer's traffic to an EU-region provider) is achievable per workspace.

Negative: an additional layer of indirection between the application and the provider SDK. New provider-specific features (a new structured-output mode, a new caching primitive) require the interface to grow before the feature is available, and there is a real temptation to take a shortcut and reach past the abstraction. We mitigate this by keeping the interface small and growing it deliberately, and by making it easy to add a typed extension method for a single provider when the feature is genuinely provider-specific.

Neutral: the interface is opinionated toward Anthropic's tool-use shape because that is closest to our agent runtime's model. This is intentional. If Anthropic's shape becomes a poor fit in the future, the interface will be revised — but it would be a revision, not an admission of failure.

## References

- [Anthropic Messages API](https://docs.anthropic.com/en/api/messages)
- [Anthropic streaming events](https://docs.anthropic.com/en/api/messages-streaming#event-types)
- [OpenAI Chat Completions streaming](https://platform.openai.com/docs/api-reference/chat-streaming)
- [LiteLLM project](https://github.com/BerriAI/litellm) — the alternative we considered and did not adopt
- [Amazon Bedrock InvokeModelWithResponseStream](https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModelWithResponseStream.html)
