package graphrag

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/ayoubzulfiqar/aerollm/internal/rag"
)

// Limits enforced by the graph handler.
const (
	MaxGraphBodyBytes = 10 << 20
	MaxGraphItems     = 1000
	MaxGraphTexts     = 100
	maxGraphIDLen     = 256
	maxGraphLabelLen  = 1024
)

type ingestNode struct {
	ID    string                 `json:"id"`
	Label string                 `json:"label"`
	Type  string                 `json:"type,omitempty"`
	Props map[string]interface{} `json:"props,omitempty"`
}

type ingestEdge struct {
	ID        string                 `json:"id"`
	Source    string                 `json:"source"`
	Target    string                 `json:"target"`
	Label     string                 `json:"label,omitempty"`
	Props     map[string]interface{} `json:"props,omitempty"`
	ValidFrom *time.Time             `json:"valid_from,omitempty"`
	ValidTo   *time.Time             `json:"valid_to,omitempty"`
}

type ingestRequest struct {
	Nodes []ingestNode `json:"nodes"`
	Edges []ingestEdge `json:"edges"`
	// Texts are run through the entity extractor (AutoOntologyWorker).
	Texts []string `json:"texts"`
}

// NewGraphHandler returns an admin HTTP handler that populates and inspects
// a graph store:
//
//	POST {"nodes":[{"id","label","type","props"}],
//	      "edges":[{"id","source","target","label","props","valid_from","valid_to"}],
//	      "texts":["free text to extract entities from"]}
//	     -> 200 {"nodes":[ids],"edges":[ids],"entities":N}
//	GET  -> 200 {"nodes":N,"edges":M} (when the store can count)
//
// Nodes are upserted before edges, then texts are ingested with worker
// (a heuristic extractor over store when worker is nil). Mount it behind
// admin authentication.
func NewGraphHandler(store GraphStore, worker *AutoOntologyWorker) http.Handler {
	if worker == nil {
		worker = NewAutoOntologyWorker(store, nil)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			out := map[string]interface{}{}
			if c, ok := store.(interface{ Counts() (int, int) }); ok {
				out["nodes"], out["edges"] = c.Counts()
			}
			writeGraphJSON(w, http.StatusOK, out)
		case http.MethodPost:
			ingestGraph(w, r, store, worker)
		default:
			w.Header().Set("Allow", "GET, POST")
			writeGraphError(w, http.StatusMethodNotAllowed, "method not allowed")
		}
	})
}

func ingestGraph(w http.ResponseWriter, r *http.Request, store GraphStore, worker *AutoOntologyWorker) {
	if store == nil {
		writeGraphError(w, http.StatusServiceUnavailable, "graph store not configured")
		return
	}
	if !rag.IsJSONRequest(r) {
		writeGraphError(w, http.StatusUnsupportedMediaType, "content type must be application/json")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxGraphBodyBytes))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeGraphError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeGraphError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	var req ingestRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeGraphError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(req.Nodes)+len(req.Edges)+len(req.Texts) == 0 {
		writeGraphError(w, http.StatusBadRequest, "no nodes, edges or texts supplied")
		return
	}
	if len(req.Nodes) > MaxGraphItems || len(req.Edges) > MaxGraphItems || len(req.Texts) > MaxGraphTexts {
		writeGraphError(w, http.StatusRequestEntityTooLarge, "too many items in one request")
		return
	}
	for _, n := range req.Nodes {
		if !validGraphText(n.ID, maxGraphIDLen) || !validGraphText(n.Label, maxGraphLabelLen) || !validGraphText(n.Type, maxGraphIDLen) {
			writeGraphError(w, http.StatusBadRequest, "node id/label/type invalid or too long")
			return
		}
		if n.ID == "" && strings.TrimSpace(n.Label) == "" {
			writeGraphError(w, http.StatusBadRequest, "node needs an id or a label")
			return
		}
	}
	for _, e := range req.Edges {
		if e.Source == "" || e.Target == "" {
			writeGraphError(w, http.StatusBadRequest, "edge needs a source and a target")
			return
		}
		if !validGraphText(e.ID, maxGraphIDLen) || !validGraphText(e.Source, maxGraphIDLen) ||
			!validGraphText(e.Target, maxGraphIDLen) || !validGraphText(e.Label, maxGraphLabelLen) {
			writeGraphError(w, http.StatusBadRequest, "edge id/source/target/label invalid or too long")
			return
		}
		if e.ValidFrom != nil && e.ValidTo != nil && e.ValidTo.Before(*e.ValidFrom) {
			writeGraphError(w, http.StatusBadRequest, "edge valid_to precedes valid_from")
			return
		}
	}
	for _, t := range req.Texts {
		if !utf8.ValidString(t) || len(t) > 4*maxIngestChars {
			writeGraphError(w, http.StatusBadRequest, "texts must be valid UTF-8 of bounded size")
			return
		}
	}

	ctx := r.Context()
	nodeIDs := make([]string, 0, len(req.Nodes))
	for _, n := range req.Nodes {
		id, err := store.UpsertNode(ctx, Node{ID: n.ID, Label: n.Label, Type: n.Type, Props: n.Props})
		if err != nil {
			writeGraphError(w, http.StatusInternalServerError, "failed to store node")
			return
		}
		nodeIDs = append(nodeIDs, id)
	}
	edgeIDs := make([]string, 0, len(req.Edges))
	for _, e := range req.Edges {
		edge := Edge{ID: e.ID, Source: e.Source, Target: e.Target, Label: e.Label, Props: e.Props, ValidTo: e.ValidTo}
		if e.ValidFrom != nil {
			edge.ValidFrom = *e.ValidFrom
		}
		id, err := store.UpsertEdge(ctx, edge)
		if err != nil {
			writeGraphError(w, http.StatusInternalServerError, "failed to store edge")
			return
		}
		edgeIDs = append(edgeIDs, id)
	}
	entities := 0
	for _, t := range req.Texts {
		n, err := worker.IngestText(ctx, t)
		entities += n
		if err != nil {
			writeGraphError(w, http.StatusInternalServerError, "failed to ingest text")
			return
		}
	}
	writeGraphJSON(w, http.StatusOK, map[string]interface{}{"nodes": nodeIDs, "edges": edgeIDs, "entities": entities})
}

func validGraphText(s string, max int) bool {
	return len(s) <= max && utf8.ValidString(s)
}

func writeGraphJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeGraphError(w http.ResponseWriter, status int, msg string) {
	writeGraphJSON(w, status, map[string]interface{}{"error": map[string]interface{}{"message": msg, "code": status}})
}
