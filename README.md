# Workspace Event Protocol
## Specification 0.1

**Status:** Draft  
**Authors:** Nathan Rose  
**Repository:** https://github.com/nanotasher/wep  
**License:** Apache-2.0

---

## Abstract

The Workspace Event Protocol (WEP) defines a standard for structured event delivery from an AI agent backend to any connected consumer. WEP enables an agent to communicate text output, operational status, and structured data payloads over a streaming connection in a consistent, consumer-agnostic way.

WEP is a data delivery protocol, well-suited for driving UI frameworks but not limited to that purpose. A consumer may render events visually, use them as additional context, feed them into downstream API calls, log them, or ignore them entirely. The protocol is concerned only with the reliable, structured delivery of typed events and the data they carry.

WEP is agent-agnostic, domain-agnostic, and transport-agnostic.

---

## 1. Motivation

AI agent backends produce multiple types of output during a single response turn: conversational text, tool invocation status, and structured data results. Conventional streaming interfaces treat all of this as a single undifferentiated text stream, forcing consumers to parse, infer, and guess at structure that the backend already knows.

WEP addresses this by defining a typed event stream in which each event carries explicit type information, a correlation identifier, and a structured data payload. Consumers can route, filter, and act on events deterministically without parsing text or inferring intent.

A secondary goal of WEP is backward compatibility. Consumers that do not implement WEP signal handling will receive text events normally and can ignore all other event types without breakage.

---

## 2. Terminology

| Term | Definition |
|------|-----------|
| **Agent** | The AI backend producing the WEP event stream |
| **Consumer** | Any client receiving and processing the WEP event stream |
| **Turn** | A single request-response cycle between a user and the agent |
| **Stream** | The SSE connection carrying WEP events for a turn or session |
| **Signal** | A structured event carrying operational status or a data payload |
| **Correlation ID** | A unique identifier assigned to each event, used to relate events within and across turns |
| **stream_id** | An identifier scoped to a session, correlating open, update, and close events for a single data payload |
| **Profile** | A WEP connection model defining stream lifecycle behavior |
| **Request-response profile** | A profile in which the stream opens per turn and closes on `[DONE]` |
| **Persistent session profile** | A profile in which the stream remains open across turns |

---

## 3. CloudEvents Adoption

WEP fully adopts the **CloudEvents 1.0 specification** (https://cloudevents.io). Every WEP event is a valid CloudEvent. CloudEvents is not an optional compatibility layer — it is the event envelope that WEP is built on.

This adoption has a practical consequence: any tool, platform, or library that speaks CloudEvents can receive, route, filter, store, or forward WEP events using standard CloudEvents tooling — without any WEP-specific code. For example:

- A CloudEvents-compatible logging pipeline will automatically capture and index all WEP events by type, source, and timestamp
- A CloudEvents router can forward `wep.stream.error` events to an alerting system
- A CloudEvents-compatible observability platform will display the full event envelope correctly

This works because CloudEvents consumers route by the `type` field. Since all WEP event types are namespaced under `wep.stream.*`, any CloudEvents consumer can filter or route WEP events using standard mechanisms. WEP-unaware consumers receive and process the envelope correctly; they simply have no handling for WEP-specific `data` payloads, which they will ignore.

### 3.1 Required CloudEvents Attributes

All WEP events MUST include the following CloudEvents attributes:

| Attribute | Description |
|-----------|-------------|
| `specversion` | Always `"1.0"` |
| `type` | WEP event type (see Section 5) |
| `source` | Agent identifier, e.g. `"mia-api/sessions/abc123"` |
| `id` | Unique event identifier. Serves as the WEP Correlation ID. |
| `time` | ISO 8601 timestamp of event emission |
| `datacontenttype` | Always `"application/json"` |
| `data` | WEP event payload (type-specific, see Section 6) |

**Example CloudEvents-conformant WEP event:**

```json
{
  "specversion": "1.0",
  "type": "wep.stream.text",
  "source": "mia-api/sessions/abc123",
  "id": "evt_0042",
  "time": "2026-06-06T00:21:00Z",
  "datacontenttype": "application/json",
  "data": {
    "text": "Of course, let me look into that."
  }
}
```

---

## 4. Transport

WEP is designed for use with **Server-Sent Events (SSE)** over HTTP. Each WEP event is transmitted as a standard SSE data frame:

```
data: <cloudevents-json>\n\n
```

### 4.1 Backward Compatibility

WEP events are JSON objects in standard SSE data frames. Consumers that do not implement WEP will receive `wep.stream.text` events as unrecognized JSON and may handle them however they choose. All other WEP event types will similarly arrive as unrecognized JSON and SHOULD be silently ignored. No consumer breakage occurs as a result of a server emitting WEP events to a non-WEP consumer.

### 4.2 Stream Termination

Every WEP stream MUST be terminated with the following sentinel frame:

```
data: [DONE]\n\n
```

A WEP-compliant server MUST emit `[DONE]` as the final frame of every turn, including turns that terminate due to an error. This guarantee MUST be enforced via a `finally` block or equivalent construct in the streaming generator to ensure delivery under all exit conditions.

`[DONE]` signals end-of-turn. Its effect on the stream connection is profile-dependent (see Section 7).

### 4.3 Version Header

WEP servers MUST include the following HTTP response header on all SSE responses:

```
wep-version: 0.1
```

Consumers receiving an unknown major version SHOULD log a warning. Consumers receiving a higher minor version MUST process all known event types and ignore unknown ones.

---

## 5. Event Types

All WEP event types use the namespace `wep.stream`.

| Type | Description |
|------|-------------|
| `wep.stream.text` | A text delta comprising part of the agent's conversational output |
| `wep.stream.working` | The agent has begun a background operation |
| `wep.stream.working_done` | A background operation completed successfully |
| `wep.stream.working_error` | A background operation failed |
| `wep.stream.open` | The agent is delivering a named, typed data payload |
| `wep.stream.update` | The agent is updating a previously opened data payload |
| `wep.stream.close` | The agent signals that a named data payload is no longer relevant |
| `wep.stream.error` | An unrecoverable error has occurred in the agent backend |
| `wep.stream.done` | Typed synonym for the `[DONE]` sentinel; MAY be emitted before the sentinel frame |

---

## 6. Event Payloads

The CloudEvents `data` field carries a WEP payload object. Payload schemas are defined per event type below.

### 6.1 `wep.stream.text`

A fragment of the agent's conversational text output. Consumers appending text deltas in order will reconstruct the full response.

```json
{
  "text": "string"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `text` | Yes | Text fragment to append to the current response |

### 6.2 `wep.stream.working`

Signals that the agent has initiated a background operation. Emitted immediately before a tool or skill invocation begins. The turn does not terminate on receipt of this event — the agent MAY emit further events including additional tool calls and text output after the operation completes.

Both the agent backend and any connected consumer observe this event. The agent MAY initiate further operations in response to a subsequent `wep.stream.working_error` — for example, querying diagnostic systems after a failed data fetch — without any instruction from the consumer.

```json
{
  "label": "string",
  "detail": "string | null",
  "ref": "string | null"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `label` | Yes | Short human-readable description of the operation |
| `detail` | No | Optional secondary detail |
| `ref` | No | Identifier correlating this event to its corresponding `working_done` or `working_error` |

### 6.3 `wep.stream.working_done`

Signals that a background operation completed successfully.

```json
{
  "label": "string | null",
  "ref": "string | null"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `label` | No | Optional resolved status label |
| `ref` | No | Correlates to the `ref` of the preceding `wep.stream.working` event |

### 6.4 `wep.stream.working_error`

Signals that a background operation failed. The turn continues — the agent determines the appropriate next action.

```json
{
  "label": "string",
  "message": "string | null",
  "ref": "string | null"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `label` | Yes | Short error summary |
| `message` | No | Optional technical detail |
| `ref` | No | Correlates to the `ref` of the preceding `wep.stream.working` event |

### 6.5 `wep.stream.open`

Signals that the agent is delivering a named, typed data payload. The `stream_id` field identifies this payload for subsequent `update` and `close` events within the session.

What the consumer does with the payload is outside the scope of this specification. Consumers MAY render it visually, pass it to a downstream system, use it as context, or ignore it.

```json
{
  "stream_id": "string",
  "kind": "string",
  "title": "string | null",
  "data": "object | null",
  "size": "string | null"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `stream_id` | Yes | Stable identifier for this data stream, unique within the session |
| `kind` | Yes | Consumer-defined type identifier describing the nature of the data. WEP does not define or constrain kind values. |
| `title` | No | Optional human-readable label for this data stream |
| `data` | No | Initial data payload. Schema is determined by `kind` and is outside the scope of this specification. |
| `size` | No | Optional hint whose semantics are defined by the consumer implementation |

### 6.6 `wep.stream.update`

Updates the data payload of a previously opened stream.

```json
{
  "stream_id": "string",
  "title": "string | null",
  "data": "object | null"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `stream_id` | Yes | Must match the `stream_id` of a previously emitted `wep.stream.open` event |
| `title` | No | Updated title, if changed |
| `data` | No | Updated data payload |

Servers MUST NOT emit `wep.stream.update` for a `stream_id` that has not been opened with `wep.stream.open` in the current session.

### 6.7 `wep.stream.close`

Signals that a named data stream is no longer relevant. Consumer behavior on receipt of this event is implementation-defined.

```json
{
  "stream_id": "string"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `stream_id` | Yes | Must match the `stream_id` of a previously emitted `wep.stream.open` event |

Servers MUST NOT emit `wep.stream.close` for a `stream_id` that has not been opened in the current session.

### 6.8 `wep.stream.error`

Signals an unrecoverable error in the agent backend. The turn terminates after this event and `[DONE]` will follow immediately.

```json
{
  "message": "string"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `message` | Yes | Human-readable error description |

---

## 7. Connection Profiles

WEP defines two connection profiles. Implementations MUST declare which profile they use.

### 7.1 Request-Response Profile

The stream opens when a turn begins and closes when `[DONE]` is received. Each user input initiates a new HTTP SSE request. The server closes the connection after emitting `[DONE]`. The client returns to a ready state and opens a new connection on the next user input.

This profile is suitable for implementations that do not require server-initiated communication.

**Turn lifecycle:**

```
User submits input
  → Client opens SSE request to agent endpoint
  → Server emits wep.stream.* events
  → Server emits [DONE]
  → Connection closes
  → Client returns to ready state
  → Next user input opens a new connection
```

### 7.2 Persistent Session Profile

The stream remains open across turns. `[DONE]` signals end-of-turn but does not close the connection. The server MAY emit new turn events at any time while the connection is open.

Connection recovery on unexpected disconnection is the responsibility of the consumer implementation.

Server-initiated turns — in which the agent begins a turn without user input — are valid in this profile and will be defined in a future version of this specification.

**Turn lifecycle:**

```
Session begins
  → Client opens persistent SSE connection to agent endpoint
  → Connection remains open across turns

Per turn:
  → Turn begins on user input
  → Server emits wep.stream.* events
  → Server emits [DONE]
  → Connection remains open
  → Client returns to ready state
  → Next turn begins on next user input
```

---

## 8. Compliance

### 8.1 Server Compliance

A WEP-compliant server MUST:

1. Emit all events as CloudEvents 1.0 conformant JSON in SSE data frames
2. Include the `wep-version` HTTP response header on all SSE responses
3. Always emit `[DONE]` as the final frame of every turn
4. Assign a unique `id` to every event
5. Include `specversion`, `source`, `time`, and `datacontenttype` on every event
6. Not emit `wep.stream.update` or `wep.stream.close` for a `stream_id` not opened in the current session
7. Declare which connection profile the implementation uses

A WEP-compliant server SHOULD:

1. Emit `wep.stream.working` before every tool or skill invocation
2. Emit `wep.stream.working_done` or `wep.stream.working_error` after every tool or skill invocation
3. Include human-readable `label` fields on all working events
4. Use the `ref` field to correlate working, done, and error event pairs

### 8.2 Consumer Compliance

A WEP-compliant consumer MUST:

1. Parse each SSE data frame as a CloudEvents JSON object
2. Route events by the CloudEvents `type` field
3. Cease processing turn output on receipt of `[DONE]`
4. Handle unknown event types gracefully — log and ignore
5. Handle unknown `kind` values in `wep.stream.open` events gracefully

A WEP-compliant consumer SHOULD:

1. Verify the `wep-version` header and log a warning on version mismatch
2. Use the CloudEvents `id` field as the correlation identifier for event tracking
3. Use the `ref` field to correlate working, done, and error event pairs

---

## Appendix A — Complete Stream Example

The following example shows a complete WEP stream for a single request-response turn involving a tool call and data payload delivery. Events are compacted for readability.

```
data: {"specversion":"1.0","type":"wep.stream.text","source":"agent/sessions/s1","id":"evt_001","time":"2026-06-06T00:21:00Z","datacontenttype":"application/json","data":{"text":"Of course. Let me pull that up."}}

data: {"specversion":"1.0","type":"wep.stream.working","source":"agent/sessions/s1","id":"evt_002","time":"2026-06-06T00:21:01Z","datacontenttype":"application/json","data":{"label":"Fetching data","ref":"work_001"}}

data: {"specversion":"1.0","type":"wep.stream.working_done","source":"agent/sessions/s1","id":"evt_003","time":"2026-06-06T00:21:03Z","datacontenttype":"application/json","data":{"label":"Data loaded","ref":"work_001"}}

data: {"specversion":"1.0","type":"wep.stream.open","source":"agent/sessions/s1","id":"evt_004","time":"2026-06-06T00:21:03Z","datacontenttype":"application/json","data":{"stream_id":"payload-001","kind":"chart","title":"Portfolio Overview","data":{}}}

data: {"specversion":"1.0","type":"wep.stream.text","source":"agent/sessions/s1","id":"evt_005","time":"2026-06-06T00:21:03Z","datacontenttype":"application/json","data":{"text":"Here is your current breakdown."}}

data: [DONE]
```

---

## Appendix B — Glossary

| Term | Definition |
|------|-----------|
| SSE | Server-Sent Events. An HTTP-based protocol for unidirectional server-to-client event streaming. |
| CloudEvents | A CNCF specification defining a common envelope for events. https://cloudevents.io |
| WEP stream | An SSE stream that conforms to the Workspace Event Protocol. |
| Turn | A single request-response cycle: one user input and the agent's complete response. |
| stream_id | An identifier scoped to a session, correlating open, update, and close events for a single data payload. |
| Correlation ID | The CloudEvents `id` field. Uniquely identifies each event for tracking and routing. |
| ref | An optional identifier correlating a working event to its corresponding done or error event. |
| Request-response profile | WEP connection model in which the stream closes after each turn. |
| Persistent session profile | WEP connection model in which the stream remains open across turns. |

---

*Workspace Event Protocol 0.1 — nanotasher/wep*
