package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/spf13/cobra"
)

// Limits enforced by the gateway's /v1/rag/documents handler.
const (
	maxRAGDocuments   = 1000
	maxRAGDocBytes    = 256 << 10
	maxRAGIDLen       = 256
	maxRAGIngestBytes = 10 << 20
)

// ragDocument is one document for POST /v1/rag/documents.
type ragDocument struct {
	ID       string         `json:"id,omitempty"`
	Content  string         `json:"content"`
	Source   string         `json:"source,omitempty"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

func newRAGCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rag",
		Short: "Add, delete and count RAG documents (/v1/rag/documents, admin)",
		Long: `Manage the documents the gateway's retrieval-augmented generation
middleware retrieves from.`,
	}
	cmd.AddCommand(newRAGAddCmd(), newRAGDeleteCmd(), newRAGCountCmd())
	return cmd
}

func newRAGAddCmd() *cobra.Command {
	var file, text, id, source, metadata string
	cmd := &cobra.Command{
		Use:   "add",
		Short: "Index documents (POST /v1/rag/documents)",
		Long: `Index one document from --text, or many from --file. The file holds a
JSON array of documents or {"documents": [...]}, each with "content" and
optional "id", "source" and "metadata". Documents without an id get a
content-derived id.`,
		Example: `  aerollm rag add --text "Refunds are processed within 5 days." --id refunds --source faq.md
  aerollm rag add --text @handbook.txt --source handbook
  aerollm rag add --file docs.json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var docs []ragDocument
			switch {
			case file != "" && text != "":
				return errors.New("--file and --text are mutually exclusive")
			case file != "":
				if anyFlagChanged(cmd, "id", "source", "metadata") {
					return errors.New("--id, --source and --metadata apply to --text; set them per document in --file")
				}
				spec := "@" + file
				if file == "-" {
					spec = "-"
				}
				raw, err := readValueArg(cmd, spec)
				if err != nil {
					return err
				}
				if docs, err = parseRAGDocuments([]byte(raw)); err != nil {
					return fmt.Errorf("--file: %w", err)
				}
			case text != "":
				content, err := readValueArg(cmd, text)
				if err != nil {
					return err
				}
				doc := ragDocument{ID: strings.TrimSpace(id), Content: content, Source: source}
				if metadata != "" {
					raw, err := readValueArg(cmd, metadata)
					if err != nil {
						return err
					}
					if err := json.Unmarshal([]byte(raw), &doc.Metadata); err != nil || doc.Metadata == nil {
						return fmt.Errorf("--metadata must be a JSON object: %v", err)
					}
				}
				docs = []ragDocument{doc}
			default:
				return errors.New("one of --text or --file is required")
			}
			if err := validateRAGDocuments(docs); err != nil {
				return err
			}
			body := map[string]any{"documents": docs}
			if b, err := json.Marshal(body); err == nil && len(b) > maxRAGIngestBytes {
				return fmt.Errorf("request is %d bytes; the gateway accepts at most %d per call (split the file)", len(b), maxRAGIngestBytes)
			}
			return serverRequest(cmd, http.MethodPost, "/v1/rag/documents", nil, body, formatTable, nil, nil)
		},
	}
	f := cmd.Flags()
	f.StringVar(&file, "file", "", `JSON file with documents ("-" for stdin)`)
	f.StringVar(&text, "text", "", "document content (literal, @file, or - for stdin)")
	f.StringVar(&id, "id", "", "document id for --text (default: derived from content)")
	f.StringVar(&source, "source", "", "document source for --text (e.g. a file name or URL)")
	f.StringVar(&metadata, "metadata", "", "metadata JSON object for --text (or @file)")
	return cmd
}

// parseRAGDocuments accepts a JSON array or {"documents": [...]}, rejecting
// unknown fields so typos (e.g. "text" for "content") are caught locally.
func parseRAGDocuments(raw []byte) ([]ragDocument, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		var docs []ragDocument
		if err := jsonUnmarshalStrict(trimmed, &docs); err != nil {
			return nil, err
		}
		return docs, nil
	}
	var wrapper struct {
		Documents []ragDocument `json:"documents"`
	}
	if err := jsonUnmarshalStrict(trimmed, &wrapper); err != nil {
		return nil, err
	}
	return wrapper.Documents, nil
}

func validateRAGDocuments(docs []ragDocument) error {
	if len(docs) == 0 {
		return errors.New("no documents to add")
	}
	if len(docs) > maxRAGDocuments {
		return fmt.Errorf("%d documents: at most %d per call", len(docs), maxRAGDocuments)
	}
	for i, d := range docs {
		name := fmt.Sprintf("document %d", i+1)
		if d.ID != "" {
			name = fmt.Sprintf("document %q", d.ID)
		}
		switch {
		case strings.TrimSpace(d.Content) == "":
			return fmt.Errorf("%s: content is required", name)
		case len(d.Content) > maxRAGDocBytes || !utf8.ValidString(d.Content):
			return fmt.Errorf("%s: content must be valid UTF-8 of at most 256 KiB", name)
		case len(d.ID) > maxRAGIDLen || len(d.Source) > maxRAGIDLen:
			return fmt.Errorf("%s: id and source must be at most %d bytes", name, maxRAGIDLen)
		}
	}
	return nil
}

func newRAGDeleteCmd() *cobra.Command {
	var id string
	cmd := &cobra.Command{
		Use:     "delete",
		Short:   "Delete a document by id (DELETE /v1/rag/documents?id=)",
		Example: "  aerollm rag delete --id refunds",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			id = strings.TrimSpace(id)
			if id == "" {
				return errors.New("--id is required")
			}
			if len(id) > maxRAGIDLen {
				return fmt.Errorf("--id must be at most %d bytes", maxRAGIDLen)
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodDelete, "/v1/rag/documents", idQuery(id), nil)
			if err != nil {
				return err
			}
			return printDone(cmd, data, "deleted document "+id, nil)
		},
	}
	cmd.Flags().StringVar(&id, "id", "", "document id")
	return cmd
}

func newRAGCountCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "count",
		Short: "Print the number of indexed documents (GET /v1/rag/documents)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := outputFormat(cmd, formatTable)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, "/v1/rag/documents", nil, nil)
			if err != nil {
				return err
			}
			if format == formatJSON {
				return writeRawJSON(cmd.OutOrStdout(), data)
			}
			var resp struct {
				Count *json.Number `json:"count"`
			}
			if err := json.Unmarshal(data, &resp); err != nil || resp.Count == nil {
				return fmt.Errorf("unexpected /v1/rag/documents response: %s", strings.TrimSpace(sanitizeCell(string(data))))
			}
			out := resp.Count.String()
			if strings.HasPrefix(out, "-") {
				out = "unknown (no countable index)"
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), out)
			return err
		},
	}
}
