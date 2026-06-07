# Workspace Event Protocol

## Specification 0.2

**Status:** Draft  
**Authors:** Nathan Rose  
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

| Term                           | Definition                                                                                                     |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| **Agent**                      | The AI backend producing the WEP event stream                                                                  |
| **Consumer**                   | Any client receiving and processing the WEP event stream                                                       |
| **Turn**                       | A single request-response cycle between a user and the agent                                                   |
| **Stream**                     | The SSE connection carrying WEP events for a turn or session                                                   |
| **Signal**                     | A structured event carrying operational status or a data payload                                               |
| **Correlation ID**             | A unique identifier assigned to each event, used to relate events within and across turns                      |
| **work_id**                    | An identifier correlating a `wep.work.start` event to its corresponding `wep.work.stop` event                  |
| **stream_id**                  | An identifier scoped to a job, correlating open, update, and close events for a single streaming data payload  |
| **data_id**                    | An identifier for a single-shot data payload delivered via `wep.work.data.send`                                |
| **Profile**                    | A WEP connection model defining stream lifecycle behavior                                                       |
| **Request-response profile**   | A profile in which the stream opens per turn and closes on `[DONE]`                                            |
| **Persistent session profile** | A profile in which the stream remains open across turns                                                        |

---

## 3. CloudEvents Adoption

WEP fully adopts the **CloudEvents 1.0 specification** (<https://cloudevents.io>). Every WEP event is a valid CloudEvent. CloudEvents is not an optional compatibility layer — it is the event envelope that WEP is built on.

This adoption has a practical consequence: any tool, platform, or library that speaks CloudEvents can receive, route, filter, store, or forward WEP events using standard CloudEvents tooling — without any WEP-specific code. For example:

- A CloudEvents-compatible logging pipeline will automatically capture and index all WEP events by type, source, and timestamp
- A CloudEvents router can forward `wep.work.stop` events where `status.ok` is `false` to an alerting system
- A CloudEvents-compatible observability platform will display the full event envelope correctly

WEP event types are organized across three top-level namespaces: `wep.text`, `wep.work`, and `wep.session`. Data delivery events are nested under `wep.work` as `wep.work.stream.*` and `wep.work.data.*`, making their job-scoped lifecycle explicit in the type string itself. CloudEvents consumers can filter on any namespace or sub-namespace independently using standard mechanisms. WEP-unaware consumers receive and process the envelope correctly; they simply have no handling for WEP-specific `data` payloads, which they will ignore.

### 3.1 Required CloudEvents Attributes

All WEP events MUST include the following CloudEvents attributes:

| Attribute         | Description                                                |
| ----------------- | ---------------------------------------------------------- |
| `specversion`     | Always `"1.0"`                                             |
| `type`            | WEP event type (see Section 5)                             |
| `source`          | Agent identifier, e.g. `"mia-api/sessions/abc123"`         |
| `id`              | Unique event identifier. Serves as the WEP Correlation ID. |
| `time`            | ISO 8601 timestamp of event emission                       |
| `datacontenttype` | Always `"application/json"`                                |
| `data`            | WEP event payload (type-specific, see Section 6)           |

**Example CloudEvents-conformant WEP event:**

```json
{
  "specversion": "1.0",
  "type": "wep.text.delta",
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

WEP events are JSON objects in standard SSE data frames. Consumers that do not implement WEP will receive `wep.text.delta` events as unrecognized JSON and may handle them however they choose. All other WEP event types will similarly arrive as unrecognized JSON and SHOULD be silently ignored. No consumer breakage occurs as a result of a server emitting WEP events to a non-WEP consumer.

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
wep-version: 0.2
```

Consumers receiving an unknown major version SHOULD log a warning. Consumers receiving a higher minor version MUST process all known event types and ignore unknown ones.

---

## 5. Event Types

WEP event types are organized across three top-level namespaces. The hierarchy is self-describing: a consumer can infer lifecycle context from the type string alone.

```
wep.text.*
  └── wep.text.delta

wep.work.*
  ├── wep.work.start
  ├── wep.work.stop
  ├── wep.work.stream.open
  ├── wep.work.stream.update
  ├── wep.work.stream.close
  └── wep.work.data.send

wep.session.*
  ├── wep.session.error
  └── wep.session.done
```

`wep.text.*` events are independent and may occur at any point during a turn. `wep.work.stream.*` and `wep.work.data.*` events are only valid within the lifecycle of a `wep.work.start` / `wep.work.stop` pair.

| Type                      | Description                                                                       |
| ------------------------- | --------------------------------------------------------------------------------- |
| `wep.text.delta`          | A text fragment comprising part of the agent's conversational output              |
| `wep.work.start`          | The agent has begun a background operation                                        |
| `wep.work.stop`           | A background operation has ended; status field indicates success or failure       |
| `wep.work.stream.open`    | The agent is delivering a named, typed streaming data payload within a job        |
| `wep.work.stream.update`  | The agent is updating a previously opened streaming data payload                  |
| `wep.work.stream.close`   | The agent signals that a named streaming data payload is no longer relevant       |
| `wep.work.data.send`      | The agent is delivering a complete, single-shot data payload within a job         |
| `wep.session.error`       | An unrecoverable error has occurred in the agent backend                          |
| `wep.session.done`        | Typed synonym for the `[DONE]` sentinel; MAY be emitted before the sentinel frame |

---

## 6. Event Payloads

The CloudEvents `data` field carries a WEP payload object. Payload schemas are defined per event type below.

### 6.1 `wep.text.delta`

A fragment of the agent's conversational text output. Consumers appending text deltas in order will reconstruct the full response. This event type is independent of job lifecycle and may be emitted at any point during a turn.

```json
{
  "text": "string"
}
```

| Field  | Required | Description                                     |
| ------ | -------- | ----------------------------------------------- |
| `text` | Yes      | Text fragment to append to the current response |

### 6.2 `wep.work.start`

Signals that the agent has begun a background operation. Emitted immediately before a tool or skill invocation begins. The turn does not terminate on receipt of this event — the agent MAY emit further events, including `wep.work.stream.*`, `wep.work.data.*`, and `wep.text.*` events, before the operation concludes.

Every `wep.work.start` MUST be followed by a corresponding `wep.work.stop` event in the same turn. Consumers MUST NOT assume success or failure until `wep.work.stop` is received.

```json
{
  "work_id": "string",
  "label": "string",
  "detail": "string | null"
}
```

| Field     | Required | Description                                                            |
| --------- | -------- | ---------------------------------------------------------------------- |
| `work_id` | Yes      | Stable identifier correlating this event to its `wep.work.stop` event |
| `label`   | Yes      | Short human-readable description of the operation                      |
| `detail`  | No       | Optional secondary detail                                              |

### 6.3 `wep.work.stop`

Signals that a background operation has ended. This event is emitted regardless of outcome — it is the single point of closure for every job. If an error occurred, it is described in the `status` field. Consumers SHOULD treat the absence of `status.error` as an implicit success.

Every `wep.work.stop` corresponds to exactly one `wep.work.start` via `work_id`.

```json
{
  "work_id": "string",
  "label": "string | null",
  "status": {
    "ok": "boolean",
    "error": "string | null",
    "detail": "string | null"
  }
}
```

| Field           | Required | Description                                                          |
| --------------- | -------- | -------------------------------------------------------------------- |
| `work_id`       | Yes      | Correlates to the `work_id` of the preceding `wep.work.start` event |
| `label`         | No       | Optional resolved status label                                       |
| `status.ok`     | Yes      | `true` if the operation completed successfully, `false` otherwise    |
| `status.error`  | No       | Short error summary if `ok` is `false`                               |
| `status.detail` | No       | Optional technical detail about the error                            |

### 6.4 `wep.work.stream.open`

Signals that the agent is delivering a named, typed streaming data payload within the current job. The `stream_id` field identifies this payload for subsequent `wep.work.stream.update` and `wep.work.stream.close` events within the session.

This event is only valid within the lifecycle of a `wep.work.start` / `wep.work.stop` pair. What the consumer does with the payload is outside the scope of this specification. Consumers MAY render it visually, pass it to a downstream system, use it as context, or ignore it.

```json
{
  "stream_id": "string",
  "title": "string | null",
  "data": "object | null",
  "size": "string | null"
}
```

| Field       | Required | Description                                                                                          |
| ----------- | -------- | ---------------------------------------------------------------------------------------------------- |
| `stream_id` | Yes      | Stable identifier for this data stream, unique within the session                                    |
| `title`     | No       | Optional human-readable label for this data stream                                                   |
| `data`      | No       | Initial data payload. Schema is consumer-defined and outside the scope of this specification.        |
| `size`      | No       | Optional hint whose semantics are defined by the consumer implementation                             |

### 6.5 `wep.work.stream.update`

Updates the data payload of a previously opened stream. Only valid within the current job lifecycle.

```json
{
  "stream_id": "string",
  "title": "string | null",
  "data": "object | null"
}
```

| Field       | Required | Description                                                                         |
| ----------- | -------- | ----------------------------------------------------------------------------------- |
| `stream_id` | Yes      | Must match the `stream_id` of a previously emitted `wep.work.stream.open` event    |
| `title`     | No       | Updated title, if changed                                                           |
| `data`      | No       | Updated data payload                                                                |

Servers MUST NOT emit `wep.work.stream.update` for a `stream_id` that has not been opened with `wep.work.stream.open` in the current session.

### 6.6 `wep.work.stream.close`

Signals that a named data stream is no longer relevant. Consumer behavior on receipt of this event is implementation-defined. Only valid within the current job lifecycle.

```json
{
  "stream_id": "string"
}
```

| Field       | Required | Description                                                                      |
| ----------- | -------- | -------------------------------------------------------------------------------- |
| `stream_id` | Yes      | Must match the `stream_id` of a previously emitted `wep.work.stream.open` event |

Servers MUST NOT emit `wep.work.stream.close` for a `stream_id` that has not been opened in the current session.

### 6.7 `wep.work.data.send`

Delivers a complete, single-shot data payload within the current job. Unlike the `wep.work.stream.*` lifecycle, this event carries the entire payload in a single frame and requires no follow-up. Suitable for images, chart data, lists, rendered documents, or any content that does not require incremental updates.

This event is only valid within the lifecycle of a `wep.work.start` / `wep.work.stop` pair.

```json
{
  "data_id": "string",
  "title": "string | null",
  "data": "object"
}
```

| Field     | Required | Description                                                                                   |
| --------- | -------- | --------------------------------------------------------------------------------------------- |
| `data_id` | Yes      | Unique identifier for this payload within the session                                         |
| `title`   | No       | Optional human-readable label                                                                 |
| `data`    | Yes      | Complete data payload. Schema is consumer-defined and outside the scope of this specification.|

### 6.8 `wep.session.error`

Signals an unrecoverable error in the agent backend. The turn terminates after this event and `[DONE]` will follow immediately.

```json
{
  "message": "string"
}
```

| Field     | Required | Description                      |
| --------- | -------- | -------------------------------- |
| `message` | Yes      | Human-readable error description |

### 6.9 `wep.session.done`

A typed synonym for the `[DONE]` sentinel. MAY be emitted as a structured event immediately before the `[DONE]` frame. Carries no payload. Consumers MUST NOT rely on this event in place of the `[DONE]` sentinel — both may be present, but only `[DONE]` is guaranteed.

```json
{}
```

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
  → Server emits wep.* events
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
  → Server emits wep.* events
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
6. Not emit `wep.work.stream.update` or `wep.work.stream.close` for a `stream_id` not opened in the current session
7. Not emit `wep.work.stream.*` or `wep.work.data.*` events outside the lifecycle of a `wep.work.start` / `wep.work.stop` pair
8. Always emit a `wep.work.stop` for every `wep.work.start` in the same turn, regardless of outcome
9. Declare which connection profile the implementation uses

A WEP-compliant server SHOULD:

1. Emit `wep.work.start` before every tool or skill invocation
2. Emit `wep.work.stop` after every tool or skill invocation, with `status.ok` set appropriately
3. Include human-readable `label` fields on all work events
4. Use `work_id` to correlate start and stop event pairs

### 8.2 Consumer Compliance

A WEP-compliant consumer MUST:

1. Parse each SSE data frame as a CloudEvents JSON object
2. Route events by the CloudEvents `type` field
3. Cease processing turn output on receipt of `[DONE]`
4. Handle unknown event types gracefully — log and ignore
5. Not assume success or failure from a `wep.work.start` event; always wait for `wep.work.stop`

A WEP-compliant consumer SHOULD:

1. Verify the `wep-version` header and log a warning on version mismatch
2. Use the CloudEvents `id` field as the correlation identifier for event tracking
3. Use `work_id` to correlate `wep.work.start` and `wep.work.stop` event pairs
4. Use `stream_id` to correlate `wep.work.stream.open`, `wep.work.stream.update`, and `wep.work.stream.close` event sequences
5. Open a UI widget or equivalent affordance on receipt of `wep.work.start` and resolve or dismiss it on receipt of `wep.work.stop`

---

## Appendix A — Complete Stream Example

The following example shows a complete WEP stream for a single request-response turn involving a tool call and a streaming data payload. Events are compacted for readability.

```
data: {"specversion":"1.0","type":"wep.text.delta","source":"agent/sessions/s1","id":"evt_001","time":"2026-06-06T00:21:00Z","datacontenttype":"application/json","data":{"text":"Of course. Let me pull that up."}}

data: {"specversion":"1.0","type":"wep.work.start","source":"agent/sessions/s1","id":"evt_002","time":"2026-06-06T00:21:01Z","datacontenttype":"application/json","data":{"work_id":"work_001","label":"Fetching portfolio data"}}

data: {"specversion":"1.0","type":"wep.work.stream.open","source":"agent/sessions/s1","id":"evt_003","time":"2026-06-06T00:21:02Z","datacontenttype":"application/json","data":{"stream_id":"feed_001","title":"Portfolio Overview","data":{}}}

data: {"specversion":"1.0","type":"wep.work.stream.update","source":"agent/sessions/s1","id":"evt_004","time":"2026-06-06T00:21:03Z","datacontenttype":"application/json","data":{"stream_id":"feed_001","data":{...}}}

data: {"specversion":"1.0","type":"wep.work.stream.close","source":"agent/sessions/s1","id":"evt_005","time":"2026-06-06T00:21:04Z","datacontenttype":"application/json","data":{"stream_id":"feed_001"}}

data: {"specversion":"1.0","type":"wep.work.stop","source":"agent/sessions/s1","id":"evt_006","time":"2026-06-06T00:21:04Z","datacontenttype":"application/json","data":{"work_id":"work_001","status":{"ok":true}}}

data: {"specversion":"1.0","type":"wep.text.delta","source":"agent/sessions/s1","id":"evt_007","time":"2026-06-06T00:21:04Z","datacontenttype":"application/json","data":{"text":"Here is your current breakdown."}}

data: [DONE]
```

---

## Appendix B — Work and Data Lifecycle Patterns

### Asynchronous job (streaming data)

An agent operation that delivers data incrementally — for example, live telemetry or stock prices updated on a recurring interval.

```
wep.work.start          (work_id: "work_001", label: "Fetching live feed")
wep.work.stream.open    (stream_id: "feed_001", title: "AAPL — 5m")
wep.work.stream.update  (stream_id: "feed_001", data: {...})
wep.work.stream.update  (stream_id: "feed_001", data: {...})
wep.work.stream.update  (stream_id: "feed_001", data: {...})
wep.work.stream.close   (stream_id: "feed_001")
wep.work.stop           (work_id: "work_001", status: { ok: true })
```

### Synchronous job (single data payload)

An agent operation that delivers a complete result in one shot — for example, a chart, an image, or a list with clickable links.

```
wep.work.start      (work_id: "work_002", label: "Building chart")
wep.work.data.send  (data_id: "chart_001", title: "Q2 Revenue", data: {...})
wep.work.stop       (work_id: "work_002", status: { ok: true })
```

### Failed job

Error information is carried in `wep.work.stop`. Consumers do not need to handle a separate error event type for job lifecycle failures — every job closes with `wep.work.stop` regardless of outcome.

```
wep.work.start  (work_id: "work_003", label: "Fetching report")
wep.work.stop   (work_id: "work_003", status: { ok: false, error: "Upstream timeout", detail: "Service responded 504 after 10s" })
```

### Mixed turn (text interleaved with job output)

`wep.text.delta` events are independent of job lifecycle and may appear before, during, or after a job within the same turn.

```
wep.text.delta          (text: "Let me check on that.")
wep.work.start          (work_id: "work_004", label: "Querying database")
wep.work.data.send      (data_id: "result_001", title: "Q3 Results", data: {...})
wep.work.stop           (work_id: "work_004", status: { ok: true })
wep.text.delta          (text: "Here are the Q3 results.")
[DONE]
```

---

## Appendix C — Glossary

| Term                       | Definition                                                                                                     |
| -------------------------- | -------------------------------------------------------------------------------------------------------------- |
| SSE                        | Server-Sent Events. An HTTP-based protocol for unidirectional server-to-client event streaming.                |
| CloudEvents                | A CNCF specification defining a common envelope for events. <https://cloudevents.io>                           |
| WEP stream                 | An SSE stream that conforms to the Workspace Event Protocol.                                                   |
| Turn                       | A single request-response cycle: one user input and the agent's complete response.                             |
| work_id                    | An identifier correlating a `wep.work.start` event to its corresponding `wep.work.stop` event.                 |
| stream_id                  | An identifier scoped to a job, correlating open, update, and close events for a single streaming data payload. |
| data_id                    | An identifier for a single-shot data payload delivered via `wep.work.data.send`.                               |
| Correlation ID             | The CloudEvents `id` field. Uniquely identifies each event for tracking and routing.                           |
| Request-response profile   | WEP connection model in which the stream closes after each turn.                                               |
| Persistent session profile | WEP connection model in which the stream remains open across turns.                                            |

---

*Workspace Event Protocol 0.2 — nanotasher/wep*
