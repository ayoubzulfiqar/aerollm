package rag

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

// DocumentIndexer is implemented by stores that accept documents
// (InMemoryVectorStore, InMemoryKeywordIndex). Indexes that also implement
// AddContext/RemoveContext have their errors (embedding or persistence
// failures) reported by the documents handler as HTTP 500.
type DocumentIndexer interface {
	Add(doc Document)
}

type documentRemover interface {
	Remove(id string) bool
}

// contextIndexer is implemented by indexes whose writes can fail (e.g. with
// persistence enabled); the handler prefers it over Add.
type contextIndexer interface {
	AddContext(ctx context.Context, doc Document) error
}

// contextRemover is the error-reporting counterpart of documentRemover.
type contextRemover interface {
	RemoveContext(ctx context.Context, id string) (bool, error)
}

type documentCounter interface {
	Len() int
}

// Limits enforced by the documents handler.
const (
	MaxIngestBodyBytes    = 10 << 20
	MaxDocumentsPerIngest = 1000
	MaxDocumentBytes      = 256 << 10
	maxDocumentIDLen      = 256
)

type ingestDocument struct {
	ID       string                 `json:"id"`
	Content  string                 `json:"content"`
	Source   string                 `json:"source,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// NewDocumentsHandler returns an HTTP handler that manages the documents of
// the given indexes (typically the vector store and keyword index behind the
// hybrid retriever), so the RAG middleware has something to retrieve:
//
//	POST   {"documents":[{"id":"doc-1","content":"...","source":"...","metadata":{}}]}
//	       (or a bare JSON array) -> 200 {"indexed":N,"ids":[...]}
//	DELETE ?id=doc-1                -> 200 {"deleted":true} | 404
//	GET                             -> 200 {"count":N}
//
// Documents without an id get a content-derived id. Bodies are capped at
// MaxIngestBodyBytes, requests at MaxDocumentsPerIngest documents and each
// document at MaxDocumentBytes. Mount it behind admin authentication.
func NewDocumentsHandler(indexes ...DocumentIndexer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			count := -1
			for _, idx := range indexes {
				if c, ok := idx.(documentCounter); ok {
					if n := c.Len(); n > count {
						count = n
					}
				}
			}
			writeDocJSON(w, http.StatusOK, map[string]interface{}{"count": count})
		case http.MethodPost:
			ingest(w, r, indexes)
		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" || len(id) > maxDocumentIDLen {
				writeDocError(w, http.StatusBadRequest, "query parameter id is required")
				return
			}
			deleted := false
			for _, idx := range indexes {
				switch rm := idx.(type) {
				case contextRemover:
					ok, err := rm.RemoveContext(r.Context(), id)
					if err != nil {
						writeDocError(w, http.StatusInternalServerError, "failed to delete document")
						return
					}
					deleted = deleted || ok
				case documentRemover:
					if rm.Remove(id) {
						deleted = true
					}
				}
			}
			if !deleted {
				writeDocError(w, http.StatusNotFound, "document not found")
				return
			}
			writeDocJSON(w, http.StatusOK, map[string]interface{}{"deleted": true})
		default:
			w.Header().Set("Allow", "GET, POST, DELETE")
			writeDocError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func ingest(w http.ResponseWriter, r *http.Request, indexes []DocumentIndexer) {
	if !IsJSONRequest(r) {
		writeDocError(w, http.StatusUnsupportedMediaType, "content type must be application/json")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxIngestBodyBytes))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeDocError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeDocError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var docs []ingestDocument
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		err = json.Unmarshal(trimmed, &docs)
	} else {
		var wrapper struct {
			Documents []ingestDocument `json:"documents"`
		}
		err = json.Unmarshal(trimmed, &wrapper)
		docs = wrapper.Documents
	}
	if err != nil {
		writeDocError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(docs) == 0 {
		writeDocError(w, http.StatusBadRequest, "no documents supplied")
		return
	}
	if len(docs) > MaxDocumentsPerIngest {
		writeDocError(w, http.StatusRequestEntityTooLarge, "too many documents in one request")
		return
	}
	ids := make([]string, 0, len(docs))
	prepared := make([]Document, 0, len(docs))
	for _, d := range docs {
		if strings.TrimSpace(d.Content) == "" {
			writeDocError(w, http.StatusBadRequest, "document content is required")
			return
		}
		if len(d.Content) > MaxDocumentBytes || !utf8.ValidString(d.Content) {
			writeDocError(w, http.StatusBadRequest, "document content must be valid UTF-8 of at most 256 KiB")
			return
		}
		if len(d.ID) > maxDocumentIDLen || len(d.Source) > maxDocumentIDLen {
			writeDocError(w, http.StatusBadRequest, "document id/source too long")
			return
		}
		if d.ID == "" {
			sum := sha256.Sum256([]byte(d.Content))
			d.ID = "doc-" + hex.EncodeToString(sum[:12])
		}
		prepared = append(prepared, Document{ID: d.ID, Content: d.Content, Source: d.Source, Metadata: d.Metadata})
		ids = append(ids, d.ID)
	}
	for n, doc := range prepared {
		for _, idx := range indexes {
			if ci, ok := idx.(contextIndexer); ok {
				if err := ci.AddContext(r.Context(), doc); err != nil {
					// Documents before this one are fully indexed; report
					// them so the client can retry the rest.
					writeDocJSON(w, http.StatusInternalServerError, map[string]interface{}{
						"error":   map[string]interface{}{"message": "failed to index document " + doc.ID, "code": http.StatusInternalServerError},
						"indexed": n,
						"ids":     ids[:n],
					})
					return
				}
				continue
			}
			idx.Add(doc)
		}
	}
	writeDocJSON(w, http.StatusOK, map[string]interface{}{"indexed": len(prepared), "ids": ids})
}

func writeDocJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeDocError(w http.ResponseWriter, status int, msg string) {
	writeDocJSON(w, status, map[string]interface{}{"error": map[string]interface{}{"message": msg, "code": status}})
}
