# ADR-0003: Server-Sent Events for token streaming, WebSockets reserved for agent UI

## Status

Accepted — 2026-05-08

## Context

The chat experience streams LLM tokens to the browser as they arrive from the provider. The data flow is unidirectional from server to client during a generation: the user posts the message in a normal HTTP request, the server opens a stream, and tokens (plus interleaved citation events) flow until the generation completes. The client does not need to send anything back to the server during a generation; if the user cancels, that is a separate `DELETE /generations/:id` call.

Two protocols dominate this space. WebSockets give us a true bidirectional channel over a single TCP connection. Server-Sent Events give us a unidirectional server-to-client stream over plain HTTP using `text/event-stream`. The default vibe in tutorials and starter kits is WebSockets, partly because chat is presented as inherently bidirectional even when it isn't.

The non-obvious cost of WebSockets is operational. WebSockets break the assumptions of HTTP-aware infrastructure: corporate proxies frequently mangle or drop the `Upgrade` header, CDNs need explicit configuration to pass them through, load balancers have to be configured for sticky sessions or for connection-aware routing, and authentication flows that work on plain HTTP (cookies, signed headers) require additional rigor when the connection is long-lived. The benefit of WebSockets is bidirectionality, which we do not use during token streaming. Paying the operational cost without using the benefit is exactly the kind of decision a peer review should catch.

The non-obvious cost of SSE is reconnection semantics: if the connection drops mid-stream, the browser's native `EventSource` will reconnect, but the server has to handle resumption. For LLM streaming this is acceptable because a dropped generation is recoverable: the assistant message is persisted only on completion, and a reconnect can re-stream from the last persisted token (or, more pragmatically, the user retries).

We have one place where bidirectionality is the actual semantic: agent runs that stream intermediate steps and accept user interrupts ("stop", "skip", "approve") mid-flight. There the channel needs to carry messages in both directions and order matters. That is the WebSocket case.

## Decision

Use Server-Sent Events for chat token streaming and any other unidirectional server-to-client stream. The HTTP endpoint is `POST /v1/conversations/:id/messages` with `Accept: text/event-stream`; the response is a stream of named events (`token`, `citation`, `usage`, `error`, `end`). Reserve WebSockets for the bidirectional agent execution UI when that ships in a later phase.

## Alternatives Considered

**WebSockets for everything (chat and agents).** Rejected for chat for the reasons above: pays operational cost (proxy compatibility, sticky sessions, more complex auth) without using the benefit (bidirectionality). The default in starter kits is not the right default for production.

**Long polling.** Rejected because it produces a worse user experience (perceptible latency on each chunk arrival) and a worse server experience (more connections, more round-trip overhead) than either SSE or WebSockets. There is no scenario where long polling is the best answer here.

**HTTP/2 server push.** Rejected because browser support and ecosystem support never converged. Chrome dropped server push in 2022. This is not a viable option in 2026.

**gRPC server streaming.** Rejected for browser-facing endpoints because gRPC-Web requires a proxy to translate HTTP/2 to HTTP/1.1, and the dev-tools story is poor. Considered for service-to-service but our service-to-service is mostly Kafka rather than synchronous, so the case did not arise.

**Chunked transfer encoding with a custom protocol.** Rejected because we would be reinventing SSE. SSE is exactly chunked transfer encoding with a tiny standardized framing and a built-in browser client.

## Consequences

Positive: SSE is plain HTTP. Every proxy, CDN, load balancer, observability tool, and authentication middleware Just Works. The browser ships `EventSource` natively, with automatic reconnect and last-event-id support. The server is stateless: any replica can serve any stream; horizontal scaling needs no sticky-session configuration. Implementing on FastAPI is a `StreamingResponse` with the right media type — about ten lines.

Negative: SSE is unidirectional, which means cancellation of an in-flight generation is a separate HTTP call rather than an inline message. We pay one additional round trip when the user clicks "stop", which is fine for the use case but worth being explicit about. SSE also has an idle-timeout interaction with proxies that we mitigate by emitting heartbeat events every fifteen seconds.

Neutral: when WebSockets do arrive for the agent UI, we will pay the operational tax then, in the place where the bidirectional capability is the actual reason for the choice. The cost is real but bounded to one feature, and the trace and metric story for that feature will be designed around the protocol's quirks rather than retrofitted.

## References

- [HTML Living Standard — Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html)
- [Streaming HTTP responses with FastAPI — Tiangolo](https://fastapi.tiangolo.com/advanced/custom-response/#streamingresponse)
- [Why WebSockets are not a silver bullet — Daniel Compton](https://www.danielcompton.net/websockets)
- [Anthropic Messages API streaming](https://docs.anthropic.com/en/api/messages-streaming)
