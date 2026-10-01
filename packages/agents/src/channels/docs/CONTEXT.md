# Channels

The shared language between agent implementations (AIChatAgent, Think, Pi, Codex, opencode harnesses) and the interfaces people use to talk to them (browser, Slack, Telegram, email, voice).

## Conversations

**Conversation**:
The unit that owns exactly one transcript.
_Avoid_: Session, lane, thread

**Surface**:
Where a conversation appears on one interface, such as a Slack thread or a web page. A conversation can have many surfaces.
_Avoid_: Channel (for the location), destination

**Participant**:
A person or client acting in a conversation. A participant may be connected through several tabs or devices at once; the shared vocabulary never names an individual connection.
_Avoid_: User (when several people share a conversation), connection, tab

## Inbound events

**Inbound event**:
Something a participant did that the agent receives: a message, a tool result, an approval response, or a cancel. Resetting a conversation is not an inbound event.
_Avoid_: Command, operation

**Cancel**:
An inbound event asking the agent to stop a turn. The turn settles as aborted, whether it was queued, running or awaiting input.
_Avoid_: Stop, abort (as the event name)

**Message**:
An inbound event carrying participant input. Whether it steers the running turn or waits for the next one is the agent's policy.
_Avoid_: Steer (as an event kind), prompt

## Turns

**Turn**:
Everything the agent does from an inbound message until it is quiescent, including every model call and tool call in between. A conversation has at most one running turn, but any number of turns may be awaiting input.
_Avoid_: Operation, run, request

**Turn status**:
One of queued, running or settled. A settled turn has an outcome: completed, failed, aborted, or awaiting input. Only a turn awaiting input can run again, as a continuation, or be aborted.

**Continuation**:
The part of a turn that resumes when a tool result or approval response arrives for a turn settled as awaiting input.
_Avoid_: Continuation turn

**Attempt**:
One try at producing a turn. A new attempt starts from what the agent has already saved to the transcript, and replaces only the earlier attempt's unsaved output.
_Avoid_: Retry (as a noun)

**Step**:
One model call plus the tool calls it produced, as in the AI SDK. Used sparingly; the boundary does not depend on it.

## Output

**Conversation snapshot**:
The agent's current view of a conversation: its transcript plus every turn that is queued, running or awaiting input.
_Avoid_: Hydration payload, state sync

**Response**:
One streamed, replayable run of agent output within a turn. It either ends, closed by its producer, or is interrupted, because its producer is gone. How the turn went is never a property of the response.
_Avoid_: Delivery stream, reply, operation
