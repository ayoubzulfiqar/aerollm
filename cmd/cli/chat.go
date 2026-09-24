package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

// chatMessage is an OpenAI chat message.
type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// chatRequest is the OpenAI-compatible /v1/chat/completions request body.
type chatRequest struct {
	Model       string        `json:"model"`
	Messages    []chatMessage `json:"messages"`
	Stream      bool          `json:"stream,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
	MaxTokens   *int          `json:"max_tokens,omitempty"`
	TopP        *float64      `json:"top_p,omitempty"`
}

// chatCompletion is the subset of an OpenAI chat.completion response we use.
type chatCompletion struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Index   int `json:"index"`
		Message struct {
			Role      string          `json:"role"`
			Content   json.RawMessage `json:"content"`
			ToolCalls json.RawMessage `json:"tool_calls,omitempty"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage,omitempty"`
}

// chatChunk is the subset of an OpenAI chat.completion.chunk we use.
type chatChunk struct {
	Choices []struct {
		Index int `json:"index"`
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Error json.RawMessage `json:"error,omitempty"`
}

func newChatCmd() *cobra.Command {
	var (
		model, system     string
		stream            bool
		temperature, topP float64
		maxTokens         int
	)
	cmd := &cobra.Command{
		Use:   "chat PROMPT",
		Short: "Send a chat completion request (use \"-\" to read the prompt from stdin)",
		Long: `Send a prompt to POST /v1/chat/completions and print the reply.

With --stream the reply is printed token by token as Server-Sent Events
arrive. Pass "-" as the prompt to read it from stdin. With -o json the full
response (or, when streaming, each chunk as one JSON line) is printed.`,
		Example: `  aerollm chat "Explain CRDTs in one paragraph" --model gpt-4o
  aerollm chat "Write a haiku" -m claude-3-5-sonnet --stream --temperature 0.7
  git diff | aerollm chat - -m gpt-4o --system "Review this diff"`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			prompt := strings.Join(args, " ")
			if prompt == "-" {
				in, err := readAllLimited(cmd.InOrStdin(), "stdin")
				if err != nil {
					return err
				}
				prompt = in
			}
			if strings.TrimSpace(prompt) == "" {
				return errors.New("prompt is empty")
			}
			if model == "" {
				model = strings.TrimSpace(os.Getenv("AEROLLM_MODEL"))
			}
			if model == "" {
				return errors.New("--model is required (or set AEROLLM_MODEL)")
			}
			req := chatRequest{Model: model, Stream: stream}
			if system != "" {
				req.Messages = append(req.Messages, chatMessage{Role: "system", Content: system})
			}
			req.Messages = append(req.Messages, chatMessage{Role: "user", Content: prompt})
			if cmd.Flags().Changed("temperature") {
				if temperature < 0 || temperature > 2 {
					return fmt.Errorf("--temperature must be between 0 and 2, got %g", temperature)
				}
				t := temperature
				req.Temperature = &t
			}
			if cmd.Flags().Changed("top-p") {
				if topP <= 0 || topP > 1 {
					return fmt.Errorf("--top-p must be in (0, 1], got %g", topP)
				}
				p := topP
				req.TopP = &p
			}
			if cmd.Flags().Changed("max-tokens") {
				if maxTokens <= 0 {
					return fmt.Errorf("--max-tokens must be positive, got %d", maxTokens)
				}
				n := maxTokens
				req.MaxTokens = &n
			}
			format, err := outputFormat(cmd, "")
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			if stream {
				return runChatStream(cmd.Context(), client, req, cmd.OutOrStdout(), format == formatJSON)
			}
			return runChatOnce(cmd.Context(), client, req, cmd.OutOrStdout(), format == formatJSON)
		},
	}
	cmd.Flags().StringVarP(&model, "model", "m", "", "model name (env AEROLLM_MODEL)")
	cmd.Flags().StringVarP(&system, "system", "s", "", "system prompt")
	cmd.Flags().BoolVar(&stream, "stream", false, "stream tokens as they are generated (SSE)")
	cmd.Flags().Float64Var(&temperature, "temperature", 1, "sampling temperature (0-2)")
	cmd.Flags().Float64Var(&topP, "top-p", 1, "nucleus sampling probability mass (0-1]")
	cmd.Flags().IntVar(&maxTokens, "max-tokens", 0, "maximum tokens to generate")
	return cmd
}

func runChatOnce(ctx context.Context, c *apiClient, req chatRequest, w io.Writer, asJSON bool) error {
	data, err := c.call(ctx, http.MethodPost, "/v1/chat/completions", nil, req)
	if err != nil {
		return err
	}
	if asJSON {
		return writeRawJSON(w, data)
	}
	text, err := completionText(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, text)
	return err
}

// completionText extracts choices[0].message.content from a chat.completion.
// Content may be a string or an array of {"type":"text","text":...} parts.
func completionText(data []byte) (string, error) {
	var resp chatCompletion
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", fmt.Errorf("decoding chat completion: %w", err)
	}
	if len(resp.Choices) == 0 {
		if msg := extractErrorMessage(data); msg != "" && strings.Contains(string(data), `"error"`) {
			return "", fmt.Errorf("server error: %s", msg)
		}
		return "", errors.New("chat completion contained no choices")
	}
	msg := resp.Choices[0].Message
	text, err := contentText(msg.Content)
	if err != nil {
		return "", err
	}
	if text == "" && len(msg.ToolCalls) > 0 && string(msg.ToolCalls) != "null" {
		return "[tool calls] " + string(msg.ToolCalls), nil
	}
	return text, nil
}

func contentText(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("unsupported message content: %s", string(raw))
	}
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" || p.Type == "" {
			b.WriteString(p.Text)
		}
	}
	return b.String(), nil
}

// runChatStream sends a streaming request and writes content deltas (or raw
// chunks when rawChunks is set) to w as they arrive.
func runChatStream(ctx context.Context, c *apiClient, req chatRequest, w io.Writer, rawChunks bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	httpReq, err := c.newRequest(ctx, http.MethodPost, "/v1/chat/completions", nil, req)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Accept", "text/event-stream")

	// The overall client timeout would cut long generations short, so
	// streaming uses the timeout for the response headers and as an idle
	// timeout between reads instead.
	streamClient := &http.Client{Transport: streamingTransport(c.timeout)}
	sc := *c
	sc.http = streamClient
	resp, err := sc.do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body := newIdleTimeoutReader(resp.Body, c.timeout, cancel)
	defer body.stop()

	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaType == "application/json" {
		// Server ignored stream=true and answered with a full completion.
		data, err := io.ReadAll(io.LimitReader(body, maxResponseBytes))
		if err != nil {
			return body.wrapErr(err)
		}
		if rawChunks {
			return writeRawJSON(w, data)
		}
		text, err := completionText(data)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(w, text)
		return err
	}

	var onRaw func([]byte) error
	if rawChunks {
		onRaw = func(b []byte) error {
			_, err := fmt.Fprintln(w, string(b))
			return err
		}
	}
	wrote := false
	onDelta := func(s string) error {
		if rawChunks {
			return nil
		}
		wrote = true
		_, err := io.WriteString(w, s)
		return err
	}
	err = streamChatCompletion(body, onDelta, onRaw)
	if wrote {
		fmt.Fprintln(w)
	}
	return body.wrapErr(err)
}

// streamingTransport clones the default transport (keeping TLS verification,
// proxies and HTTP/2) and bounds the time to receive response headers.
func streamingTransport(timeout time.Duration) http.RoundTripper {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	t = t.Clone()
	t.ResponseHeaderTimeout = timeout
	return t
}

// sseEvent is one dispatched Server-Sent Event.
type sseEvent struct {
	Event string
	Data  string
	ID    string
}

// maxSSELine bounds a single SSE line so a hostile server cannot make the
// scanner buffer grow without limit.
const maxSSELine = 16 << 20

// readSSE parses a text/event-stream from r and calls fn for every event, per
// the WHATWG EventSource rules: "field: value" lines, a single optional space
// after the colon, ":" comment lines, multiple data lines joined with "\n",
// and a blank line dispatching the event. An event still pending at EOF is
// dispatched too, to tolerate servers that omit the final blank line. If fn
// returns errStopSSE parsing stops and readSSE returns nil.
func readSSE(r io.Reader, fn func(sseEvent) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxSSELine)
	var (
		ev      sseEvent
		data    []string
		hasData bool
	)
	dispatch := func() error {
		if !hasData {
			ev = sseEvent{}
			return nil
		}
		ev.Data = strings.Join(data, "\n")
		e := ev
		ev, data, hasData = sseEvent{}, data[:0], false
		return fn(e)
	}
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				if errors.Is(err, errStopSSE) {
					return nil
				}
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue // comment / keep-alive
		}
		field, value, found := strings.Cut(line, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "data":
			data = append(data, value)
			hasData = true
		case "event":
			ev.Event = value
		case "id":
			ev.ID = value
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if err := dispatch(); err != nil && !errors.Is(err, errStopSSE) {
		return err
	}
	return nil
}

var errStopSSE = errors.New("stop sse")

// errStreamTruncated is returned when a chat stream ends without [DONE].
var errStreamTruncated = errors.New("stream ended before [DONE]; the response may be incomplete")

// streamChatCompletion consumes an OpenAI chat.completion.chunk SSE stream.
// onDelta receives choices[0].delta.content fragments in order; onRaw (if not
// nil) receives each raw chunk JSON. An {"error":...} event aborts the
// stream with that error. A stream that ends without "data: [DONE]" and
// without a finish_reason for choice 0 returns errStreamTruncated.
func streamChatCompletion(r io.Reader, onDelta func(string) error, onRaw func([]byte) error) error {
	done, finished := false, false
	err := readSSE(r, func(ev sseEvent) error {
		payload := strings.TrimSpace(ev.Data)
		if payload == "" {
			return nil
		}
		if payload == "[DONE]" {
			done = true
			return errStopSSE
		}
		if ev.Event == "error" {
			return fmt.Errorf("stream error: %s", extractErrorMessage([]byte(payload)))
		}
		var chunk chatChunk
		if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
			return fmt.Errorf("invalid stream chunk: %w", err)
		}
		if len(chunk.Error) > 0 && string(chunk.Error) != "null" {
			return fmt.Errorf("stream error: %s", extractErrorMessage([]byte(payload)))
		}
		if onRaw != nil {
			if err := onRaw([]byte(payload)); err != nil {
				return err
			}
		}
		for _, ch := range chunk.Choices {
			if ch.Index == 0 && ch.FinishReason != nil && *ch.FinishReason != "" {
				finished = true
			}
			if ch.Index != 0 || ch.Delta.Content == "" || onDelta == nil {
				continue
			}
			if err := onDelta(ch.Delta.Content); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !done && !finished {
		return errStreamTruncated
	}
	return nil
}

// idleTimeoutReader cancels the request if no data arrives for timeout.
type idleTimeoutReader struct {
	r       io.Reader
	timeout time.Duration
	timer   *time.Timer
	mu      sync.Mutex
	fired   bool
}

func newIdleTimeoutReader(r io.Reader, timeout time.Duration, cancel context.CancelFunc) *idleTimeoutReader {
	ir := &idleTimeoutReader{r: r, timeout: timeout}
	ir.timer = time.AfterFunc(timeout, func() {
		ir.mu.Lock()
		ir.fired = true
		ir.mu.Unlock()
		cancel()
	})
	return ir
}

func (ir *idleTimeoutReader) Read(p []byte) (int, error) {
	n, err := ir.r.Read(p)
	if n > 0 {
		ir.timer.Reset(ir.timeout)
	}
	return n, err
}

func (ir *idleTimeoutReader) stop() { ir.timer.Stop() }

// wrapErr turns the cancellation caused by the idle timer into a clear error.
func (ir *idleTimeoutReader) wrapErr(err error) error {
	if err == nil {
		return nil
	}
	ir.mu.Lock()
	fired := ir.fired
	ir.mu.Unlock()
	if fired {
		return fmt.Errorf("stream idle for more than %s: %w", ir.timeout, err)
	}
	return err
}
