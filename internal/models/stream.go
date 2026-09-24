package models

// StreamChunk is one OpenAI-compatible "chat.completion.chunk" event as
// emitted over Server-Sent Events by /v1/chat/completions with stream=true.
//
// Producers (providers implementing providers.StreamingProvider) send chunks
// on a channel and close it when the stream ends. A chunk whose Err is non-nil
// signals an upstream failure and must be the last chunk sent.
type StreamChunk struct {
	ID      string         `json:"id"`
	Object  string         `json:"object"`
	Created int64          `json:"created"`
	Model   string         `json:"model"`
	Choices []StreamChoice `json:"choices"`
	// Usage is populated on the final chunk when the upstream reports it.
	Usage *Usage `json:"usage,omitempty"`

	// Err reports a mid-stream failure. It is never serialized.
	Err error `json:"-"`
}

// StreamChoice is a single choice delta within a StreamChunk.
type StreamChoice struct {
	Index        int          `json:"index"`
	Delta        MessageDelta `json:"delta"`
	FinishReason *string      `json:"finish_reason"`
}

// MessageDelta is the incremental message content carried by a StreamChoice.
type MessageDelta struct {
	Role      MessageRole     `json:"role,omitempty"`
	Content   string          `json:"content,omitempty"`
	ToolCalls []ToolCallDelta `json:"tool_calls,omitempty"`
}

// ToolCallDelta is an incremental tool call fragment. Fragments with the same
// Index belong to the same tool call; Arguments are concatenated in order.
type ToolCallDelta struct {
	Index    int               `json:"index"`
	ID       string            `json:"id,omitempty"`
	Type     string            `json:"type,omitempty"`
	Function ToolFunctionDelta `json:"function"`
}

// ToolFunctionDelta is the function portion of a ToolCallDelta.
type ToolFunctionDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// StreamChunksFromResponse converts a complete response into the equivalent
// sequence of stream chunks (role, content, tool calls, finish). It lets the
// gateway serve stream=true requests from providers that only support
// non-streaming completions, and from cache hits.
func StreamChunksFromResponse(resp *LLMResponse) []StreamChunk {
	if resp == nil {
		return nil
	}
	base := func() StreamChunk {
		return StreamChunk{ID: resp.ID, Object: "chat.completion.chunk", Created: resp.Created, Model: resp.Model}
	}
	var chunks []StreamChunk
	for _, ch := range resp.Choices {
		role := ch.Message.Role
		if role == "" {
			role = RoleAssistant
		}
		c := base()
		c.Choices = []StreamChoice{{Index: ch.Index, Delta: MessageDelta{Role: role}}}
		chunks = append(chunks, c)

		if ch.Message.Content != nil && *ch.Message.Content != "" {
			c = base()
			c.Choices = []StreamChoice{{Index: ch.Index, Delta: MessageDelta{Content: *ch.Message.Content}}}
			chunks = append(chunks, c)
		}
		if len(ch.Message.ToolCalls) > 0 {
			deltas := make([]ToolCallDelta, len(ch.Message.ToolCalls))
			for i, tc := range ch.Message.ToolCalls {
				typ := tc.Type
				if typ == "" {
					typ = "function"
				}
				deltas[i] = ToolCallDelta{
					Index:    i,
					ID:       tc.ID,
					Type:     typ,
					Function: ToolFunctionDelta{Name: tc.Function.Name, Arguments: tc.Function.Arguments},
				}
			}
			c = base()
			c.Choices = []StreamChoice{{Index: ch.Index, Delta: MessageDelta{ToolCalls: deltas}}}
			chunks = append(chunks, c)
		}
		finish := ch.FinishReason
		if finish == "" {
			finish = "stop"
		}
		c = base()
		c.Choices = []StreamChoice{{Index: ch.Index, Delta: MessageDelta{}, FinishReason: &finish}}
		chunks = append(chunks, c)
	}
	if resp.Usage != nil && len(chunks) > 0 {
		chunks[len(chunks)-1].Usage = resp.Usage
	}
	return chunks
}
