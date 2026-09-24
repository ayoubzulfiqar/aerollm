// Package k8stest provides an in-memory fake Kubernetes API server for
// tests of the stdlib Kubernetes client in internal/k8s and the operator.
//
// It serves, over TLS: paginated list, watch (resourceVersion resume,
// BOOKMARK events, 410 Gone for compacted versions), get, status
// merge-patch and core/v1 Secrets, and records every request.
package k8stest

import (
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GVR identifies a collection.
type GVR struct {
	Group, Version, Resource string
}

func (g GVR) key() string { return g.Group + "/" + g.Version + "/" + g.Resource }

// Request is a recorded API request.
type Request struct {
	Method string
	Path   string
	Query  string
	Auth   string
	Body   []byte
}

// Patch is a recorded status patch.
type Patch struct {
	Resource  string
	Namespace string
	Name      string
	Body      []byte
}

type event struct {
	rv   int64
	coll string
	ns   string
	typ  string
	obj  map[string]interface{}
}

type watcher struct {
	coll string
	ns   string
	ch   chan event
	done chan struct{}
	once sync.Once
}

func (w *watcher) stop() { w.once.Do(func() { close(w.done) }) }

// Server is the fake API server.
type Server struct {
	// URL is the https base URL.
	URL string
	srv *httptest.Server

	mu        sync.Mutex
	token     string
	rv        int64
	minRV     int64
	objects   map[string]map[string]interface{} // coll|ns/name -> object
	secrets   map[string]map[string][]byte      // ns/name -> data
	events    []event
	watchers  map[*watcher]struct{}
	requests  []Request
	patches   []Patch
	failWatch []int
	uid       int
}

// NewTLSServer starts a fake API server. Close it when done.
func NewTLSServer() *Server {
	s := &Server{
		rv:       100,
		objects:  make(map[string]map[string]interface{}),
		secrets:  make(map[string]map[string][]byte),
		watchers: make(map[*watcher]struct{}),
	}
	s.srv = httptest.NewTLSServer(http.HandlerFunc(s.serve))
	s.URL = s.srv.URL
	return s
}

// Close shuts the server down.
func (s *Server) Close() {
	s.CloseWatches()
	s.srv.Close()
}

// CAPEM returns the PEM encoded server certificate (to use as CA data).
func (s *Server) CAPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.srv.Certificate().Raw})
}

// SetToken requires "Authorization: Bearer <token>" on every request
// ("" disables authentication).
func (s *Server) SetToken(token string) {
	s.mu.Lock()
	s.token = token
	s.mu.Unlock()
}

// ResourceVersion returns the current resourceVersion.
func (s *Server) ResourceVersion() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strconv.FormatInt(s.rv, 10)
}

func clone(obj map[string]interface{}) map[string]interface{} {
	b, err := json.Marshal(obj)
	if err != nil {
		panic(err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return out
}

func meta(obj map[string]interface{}) map[string]interface{} {
	md, _ := obj["metadata"].(map[string]interface{})
	if md == nil {
		md = map[string]interface{}{}
		obj["metadata"] = md
	}
	return md
}

func objKey(coll, ns, name string) string { return coll + "|" + ns + "/" + name }

// record appends an event and fans it out; s.mu must be held.
func (s *Server) record(coll, ns, typ string, obj map[string]interface{}) {
	ev := event{rv: s.rv, coll: coll, ns: ns, typ: typ, obj: clone(obj)}
	s.events = append(s.events, ev)
	for w := range s.watchers {
		if w.coll != coll || (w.ns != "" && w.ns != ns) {
			continue
		}
		select {
		case w.ch <- ev:
		default:
			w.stop() // slow watcher: drop it like the real server would
		}
	}
}

// Create stores a new object (metadata.namespace/name required) and emits
// ADDED. It returns the stored copy.
func (s *Server) Create(g GVR, obj map[string]interface{}) map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj = clone(obj)
	md := meta(obj)
	ns, _ := md["namespace"].(string)
	name, _ := md["name"].(string)
	s.rv++
	s.uid++
	md["resourceVersion"] = strconv.FormatInt(s.rv, 10)
	md["generation"] = 1
	md["uid"] = fmt.Sprintf("uid-%d", s.uid)
	md["creationTimestamp"] = time.Now().UTC().Format(time.RFC3339)
	obj = clone(obj)
	s.objects[objKey(g.key(), ns, name)] = obj
	s.record(g.key(), ns, "ADDED", obj)
	return clone(obj)
}

// UpdateSpec replaces the object's spec, bumping metadata.generation when
// it changed, and emits MODIFIED.
func (s *Server) UpdateSpec(g GVR, ns, name string, spec map[string]interface{}) map[string]interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[objKey(g.key(), ns, name)]
	if !ok {
		panic("k8stest: update of missing object " + ns + "/" + name)
	}
	oldSpec, _ := json.Marshal(obj["spec"])
	newSpec, _ := json.Marshal(spec)
	obj["spec"] = clone(map[string]interface{}{"s": spec})["s"]
	md := meta(obj)
	if string(oldSpec) != string(newSpec) {
		gen, _ := md["generation"].(float64)
		md["generation"] = gen + 1
	}
	s.rv++
	md["resourceVersion"] = strconv.FormatInt(s.rv, 10)
	s.record(g.key(), ns, "MODIFIED", obj)
	return clone(obj)
}

// Delete removes an object and emits DELETED.
func (s *Server) Delete(g GVR, ns, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := objKey(g.key(), ns, name)
	obj, ok := s.objects[k]
	if !ok {
		return
	}
	delete(s.objects, k)
	s.rv++
	meta(obj)["resourceVersion"] = strconv.FormatInt(s.rv, 10)
	s.record(g.key(), ns, "DELETED", obj)
}

// DeleteSilently removes an object without emitting an event (as if the
// deletion happened while no watch was open and the history was
// compacted).
func (s *Server) DeleteSilently(g GVR, ns, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, objKey(g.key(), ns, name))
	s.rv++
}

// Object returns a copy of a stored object.
func (s *Server) Object(g GVR, ns, name string) (map[string]interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[objKey(g.key(), ns, name)]
	if !ok {
		return nil, false
	}
	return clone(obj), true
}

// SetSecret stores a core/v1 Secret.
func (s *Server) SetSecret(ns, name string, data map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secrets[ns+"/"+name] = data
}

// Compact drops the event history: watches resuming from any earlier
// resourceVersion get 410 Gone.
func (s *Server) Compact() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.minRV = s.rv
	s.events = nil
}

// CloseWatches ends every open watch stream (clients must reconnect).
func (s *Server) CloseWatches() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for w := range s.watchers {
		w.stop()
		delete(s.watchers, w)
	}
}

// Bookmark sends a BOOKMARK event at the current resourceVersion to every
// watcher of g.
func (s *Server) Bookmark(g GVR) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rv++
	ev := event{rv: s.rv, coll: g.key(), typ: "BOOKMARK", obj: map[string]interface{}{
		"kind": "Bookmark", "metadata": map[string]interface{}{"resourceVersion": strconv.FormatInt(s.rv, 10)},
	}}
	for w := range s.watchers {
		if w.coll == ev.coll {
			select {
			case w.ch <- ev:
			default:
			}
		}
	}
}

// FailNextWatches makes the next watch requests fail with the given HTTP
// status codes (in order).
func (s *Server) FailNextWatches(codes ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failWatch = append(s.failWatch, codes...)
}

// Requests returns every recorded request.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// Patches returns every recorded status patch.
func (s *Server) Patches() []Patch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Patch(nil), s.patches...)
}

// WatchCount returns the number of currently open watch streams.
func (s *Server) WatchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.watchers)
}

func writeStatus(w http.ResponseWriter, code int, reason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"code": code, "reason": reason, "message": msg,
	})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type route struct {
	coll, ns, name, sub string
	secret              bool
}

// parse maps a request path to a route.
func parse(path string) (route, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var r route
	var rest []string
	switch {
	case len(parts) >= 2 && parts[0] == "api":
		r.coll = "/" + parts[1]
		rest = parts[2:]
	case len(parts) >= 3 && parts[0] == "apis":
		r.coll = parts[1] + "/" + parts[2]
		rest = parts[3:]
	default:
		return r, false
	}
	if len(rest) >= 2 && rest[0] == "namespaces" {
		r.ns = rest[1]
		rest = rest[2:]
	}
	if len(rest) == 0 || len(rest) > 3 {
		return r, false
	}
	if parts[0] == "api" {
		if rest[0] != "secrets" {
			return r, false
		}
		r.secret = true
	}
	r.coll += "/" + rest[0]
	if len(rest) > 1 {
		r.name = rest[1]
	}
	if len(rest) > 2 {
		r.sub = rest[2]
	}
	return r, true
}

func (s *Server) serve(w http.ResponseWriter, req *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(req.Body, 4<<20))
	s.mu.Lock()
	s.requests = append(s.requests, Request{Method: req.Method, Path: req.URL.Path, Query: req.URL.RawQuery, Auth: req.Header.Get("Authorization"), Body: body})
	token := s.token
	s.mu.Unlock()
	if token != "" && req.Header.Get("Authorization") != "Bearer "+token {
		writeStatus(w, http.StatusUnauthorized, "Unauthorized", "Unauthorized")
		return
	}
	r, ok := parse(req.URL.Path)
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "unknown path")
		return
	}
	switch {
	case r.secret:
		s.serveSecret(w, req, r)
	case r.name == "" && req.Method == http.MethodGet && req.URL.Query().Get("watch") == "true":
		s.serveWatch(w, req, r)
	case r.name == "" && req.Method == http.MethodGet:
		s.serveList(w, req, r)
	case r.name != "" && r.sub == "" && req.Method == http.MethodGet:
		s.mu.Lock()
		obj, ok := s.objects[objKey(r.coll, r.ns, r.name)]
		if ok {
			obj = clone(obj)
		}
		s.mu.Unlock()
		if !ok {
			writeStatus(w, http.StatusNotFound, "NotFound", r.name+" not found")
			return
		}
		writeJSON(w, obj)
	case r.name != "" && r.sub == "status" && req.Method == http.MethodPatch:
		s.servePatchStatus(w, req, r, body)
	default:
		writeStatus(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported")
	}
}

func (s *Server) serveSecret(w http.ResponseWriter, req *http.Request, r route) {
	if req.Method != http.MethodGet || r.name == "" {
		writeStatus(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "unsupported")
		return
	}
	s.mu.Lock()
	data, ok := s.secrets[r.ns+"/"+r.name]
	s.mu.Unlock()
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", "secret "+r.name+" not found")
		return
	}
	enc := make(map[string]string, len(data))
	for k, v := range data {
		enc[k] = base64.StdEncoding.EncodeToString(v)
	}
	writeJSON(w, map[string]interface{}{
		"kind": "Secret", "apiVersion": "v1",
		"metadata": map[string]interface{}{"name": r.name, "namespace": r.ns},
		"data":     enc,
	})
}

func (s *Server) matching(coll, ns string) []map[string]interface{} {
	var keys []string
	prefix := coll + "|"
	for k := range s.objects {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if ns != "" && !strings.HasPrefix(k, prefix+ns+"/") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]interface{}, 0, len(keys))
	for _, k := range keys {
		out = append(out, clone(s.objects[k]))
	}
	return out
}

func (s *Server) serveList(w http.ResponseWriter, req *http.Request, r route) {
	q := req.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset := 0
	s.mu.Lock()
	rv := s.rv
	if c := q.Get("continue"); c != "" {
		var crv int64
		if _, err := fmt.Sscanf(c, "%d:%d", &offset, &crv); err != nil {
			s.mu.Unlock()
			writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid continue token")
			return
		}
		if crv < s.minRV {
			s.mu.Unlock()
			writeStatus(w, http.StatusGone, "Expired", "continue token expired")
			return
		}
		rv = crv
	}
	items := s.matching(r.coll, r.ns)
	s.mu.Unlock()
	cont := ""
	if offset > len(items) {
		offset = len(items)
	}
	items = items[offset:]
	if limit > 0 && len(items) > limit {
		items = items[:limit]
		cont = fmt.Sprintf("%d:%d", offset+limit, rv)
	}
	writeJSON(w, map[string]interface{}{
		"kind": "List", "apiVersion": "v1",
		"metadata": map[string]interface{}{"resourceVersion": strconv.FormatInt(rv, 10), "continue": cont},
		"items":    items,
	})
}

func (s *Server) serveWatch(w http.ResponseWriter, req *http.Request, r route) {
	q := req.URL.Query()
	s.mu.Lock()
	if len(s.failWatch) > 0 {
		code := s.failWatch[0]
		s.failWatch = s.failWatch[1:]
		s.mu.Unlock()
		reason := http.StatusText(code)
		if code == http.StatusGone {
			reason = "Expired"
		}
		writeStatus(w, code, reason, "injected failure")
		return
	}
	var start int64 = -1
	if v := q.Get("resourceVersion"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			s.mu.Unlock()
			writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid resourceVersion")
			return
		}
		start = n
	}
	var backlog []event
	gone := false
	switch {
	case start >= 0 && start < s.minRV:
		gone = true
	case start >= 0:
		for _, ev := range s.events {
			if ev.rv > start && ev.coll == r.coll && (r.ns == "" || ev.ns == r.ns) {
				backlog = append(backlog, ev)
			}
		}
	default:
		for _, obj := range s.matching(r.coll, r.ns) {
			backlog = append(backlog, event{typ: "ADDED", obj: obj})
		}
	}
	wt := &watcher{coll: r.coll, ns: r.ns, ch: make(chan event, 1024), done: make(chan struct{})}
	if !gone {
		s.watchers[wt] = struct{}{}
	}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.watchers, wt)
		s.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	send := func(typ string, obj map[string]interface{}) bool {
		if err := enc.Encode(map[string]interface{}{"type": typ, "object": obj}); err != nil {
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	}
	if gone {
		send("ERROR", map[string]interface{}{"kind": "Status", "apiVersion": "v1", "status": "Failure", "code": 410, "reason": "Expired", "message": "too old resource version"})
		return
	}
	if flusher != nil {
		flusher.Flush()
	}
	for _, ev := range backlog {
		if !send(ev.typ, ev.obj) {
			return
		}
	}
	timeout := 300 * time.Second
	if ts, err := strconv.Atoi(q.Get("timeoutSeconds")); err == nil && ts > 0 {
		timeout = time.Duration(ts) * time.Second
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-req.Context().Done():
			return
		case <-wt.done:
			return
		case <-timer.C:
			return
		case ev := <-wt.ch:
			if ev.typ == "BOOKMARK" && q.Get("allowWatchBookmarks") != "true" {
				continue
			}
			if !send(ev.typ, ev.obj) {
				return
			}
		}
	}
}

func (s *Server) servePatchStatus(w http.ResponseWriter, req *http.Request, r route, body []byte) {
	if ct := req.Header.Get("Content-Type"); ct != "application/merge-patch+json" {
		writeStatus(w, http.StatusUnsupportedMediaType, "UnsupportedMediaType", "unsupported patch type "+ct)
		return
	}
	var patch map[string]interface{}
	if err := json.Unmarshal(body, &patch); err != nil {
		writeStatus(w, http.StatusBadRequest, "BadRequest", "invalid patch")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := objKey(r.coll, r.ns, r.name)
	obj, ok := s.objects[k]
	if !ok {
		writeStatus(w, http.StatusNotFound, "NotFound", r.name+" not found")
		return
	}
	s.patches = append(s.patches, Patch{Resource: r.coll, Namespace: r.ns, Name: r.name, Body: body})
	if st, ok := patch["status"]; ok {
		obj["status"] = mergePatch(obj["status"], st)
	}
	s.rv++
	meta(obj)["resourceVersion"] = strconv.FormatInt(s.rv, 10)
	s.record(r.coll, r.ns, "MODIFIED", obj)
	writeJSON(w, clone(obj))
}

// mergePatch implements RFC 7386.
func mergePatch(target, patch interface{}) interface{} {
	pm, ok := patch.(map[string]interface{})
	if !ok {
		return patch
	}
	tm, ok := target.(map[string]interface{})
	if !ok {
		tm = map[string]interface{}{}
	}
	for k, v := range pm {
		if v == nil {
			delete(tm, k)
			continue
		}
		tm[k] = mergePatch(tm[k], v)
	}
	return tm
}
