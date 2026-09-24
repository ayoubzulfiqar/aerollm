package notification

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// SendPath is the route SendHandler serves.
const SendPath = "/v1/notification/send"

// Limits for SendHandler requests.
const (
	maxSendTitleRunes = 256
	maxSendTextBytes  = 16 << 10
	maxSendLabels     = 32
	sendTimeout       = 30 * time.Second
	// manualAlertID is the alert id carried by direct channel sends.
	manualAlertID = "manual"
)

var severityRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// SendRequest is the body accepted by SendHandler. Exactly one of ChannelID
// and AlertID must be set; at least one of Title and Text is required.
type SendRequest struct {
	ChannelID string            `json:"channel_id,omitempty"`
	AlertID   string            `json:"alert_id,omitempty"`
	Title     string            `json:"title"`
	Text      string            `json:"text"`
	Severity  string            `json:"severity,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// SendResponse is returned by SendHandler. Results never contain targets,
// credentials or upstream response bodies.
type SendResponse struct {
	Status    string           `json:"status"` // delivered | failed | partial
	ChannelID string           `json:"channel_id,omitempty"`
	AlertID   string           `json:"alert_id,omitempty"`
	Delivered int              `json:"delivered"`
	Failed    int              `json:"failed"`
	Results   []DeliveryResult `json:"results"`
	Error     string           `json:"error,omitempty"`
}

func (r *SendRequest) validate() error {
	r.ChannelID = strings.TrimSpace(r.ChannelID)
	r.AlertID = strings.TrimSpace(r.AlertID)
	r.Severity = strings.ToLower(strings.TrimSpace(r.Severity))
	switch {
	case r.ChannelID != "" && r.AlertID != "":
		return invalid("", "set exactly one of channel_id and alert_id")
	case r.ChannelID == "" && r.AlertID == "":
		return invalid("", "one of channel_id or alert_id is required")
	case r.ChannelID != "" && !validID(r.ChannelID):
		return invalid("channel_id", "must match %s", idRe.String())
	case len(r.AlertID) > maxAlertIDLen:
		return invalid("alert_id", "must be at most %d characters", maxAlertIDLen)
	case strings.TrimSpace(r.Title) == "" && strings.TrimSpace(r.Text) == "":
		return invalid("", "title or text is required")
	case utf8.RuneCountInString(r.Title) > maxSendTitleRunes:
		return invalid("title", "must be at most %d characters", maxSendTitleRunes)
	case len(r.Text) > maxSendTextBytes:
		return invalid("text", "must be at most %d bytes", maxSendTextBytes)
	case r.Severity != "" && !severityRe.MatchString(r.Severity):
		return invalid("severity", "must match %s", severityRe.String())
	case len(r.Labels) > maxSendLabels:
		return invalid("labels", "must have at most %d entries", maxSendLabels)
	}
	for k, v := range r.Labels {
		if k == "" || len(k) > maxMetadataKeyLen || len(v) > maxMetadataValueLen {
			return invalid("labels", "keys must be 1-%d characters and values at most %d", maxMetadataKeyLen, maxMetadataValueLen)
		}
	}
	return nil
}

// SendHandler serves POST /v1/notification/send (admin only). Delivery is
// synchronous, bounded by a 30s timeout derived from the request context:
//
//	{"channel_id":"ch_1","title":"...","text":"...","severity":"critical"}
//	{"alert_id":"cpu.high","title":"...","text":"..."}
//
// Status codes: 200 delivered to every target; 400 invalid body; 404
// unknown channel or no enabled subscription for the alert; 405 non-POST;
// 409 channel disabled or not deliverable with the dispatcher's settings;
// 501 channel type has no backend (email/SMS not configured); 502 upstream
// delivery failed (fully or partially; per-channel classes in "results");
// 503 no dispatcher; 504 timed out. Error bodies never include targets or
// credentials.
func SendHandler(d *Dispatcher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			methodNotAllowed(w, http.MethodPost)
			return
		}
		if d == nil || d.store == nil {
			writeError(w, http.StatusServiceUnavailable, "notification dispatcher not configured")
			return
		}
		var req SendRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if err := req.validate(); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), sendTimeout)
		defer cancel()
		msg := Message{
			Title:     req.Title,
			Text:      req.Text,
			Severity:  req.Severity,
			Labels:    req.Labels,
			Timestamp: time.Now().UTC(),
		}
		if req.ChannelID != "" {
			msg.AlertID = manualAlertID
			ch, ok := d.store.GetChannel(req.ChannelID)
			if !ok {
				writeError(w, http.StatusNotFound, "channel not found")
				return
			}
			err := d.Deliver(ctx, ch, msg)
			res := DeliveryResult{ChannelID: ch.ID, Type: ch.Type, Delivered: err == nil, Error: ErrorClass(err)}
			writeSendResponse(w, SendResponse{ChannelID: ch.ID}, []DeliveryResult{res})
			return
		}
		results, _ := d.NotifyWithResults(ctx, req.AlertID, msg)
		if len(results) == 0 {
			writeError(w, http.StatusNotFound, "no enabled subscriptions for alert")
			return
		}
		writeSendResponse(w, SendResponse{AlertID: req.AlertID}, results)
	}
}

// sendStatusByClass maps a failure class to the HTTP status reported when
// every target failed with that class.
var sendStatusByClass = map[string]int{
	"not_implemented":     http.StatusNotImplemented,
	"disabled":            http.StatusConflict,
	"invalid_channel":     http.StatusConflict,
	"not_found":           http.StatusNotFound,
	"blocked_destination": http.StatusBadGateway,
	"timeout":             http.StatusGatewayTimeout,
	"canceled":            http.StatusGatewayTimeout,
	"delivery_failed":     http.StatusBadGateway,
}

var sendMessageByClass = map[string]string{
	"not_implemented":     "channel type is not configured on this gateway",
	"disabled":            "channel is disabled",
	"invalid_channel":     "channel is not deliverable with the current settings",
	"not_found":           "channel not found",
	"blocked_destination": "destination address is not allowed",
	"timeout":             "delivery timed out",
	"canceled":            "delivery canceled",
	"delivery_failed":     "delivery failed",
}

func writeSendResponse(w http.ResponseWriter, resp SendResponse, results []DeliveryResult) {
	resp.Results = results
	classes := map[string]bool{}
	for _, r := range results {
		if r.Delivered {
			resp.Delivered++
		} else {
			resp.Failed++
			classes[r.Error] = true
		}
	}
	switch {
	case resp.Failed == 0:
		resp.Status = "delivered"
		writeJSON(w, http.StatusOK, resp)
		return
	case resp.Delivered > 0:
		resp.Status = "partial"
		resp.Error = "delivery failed for some channels"
		writeJSON(w, http.StatusBadGateway, resp)
		return
	}
	resp.Status = "failed"
	status, msg := http.StatusBadGateway, "delivery failed"
	if len(classes) == 1 {
		for c := range classes {
			if s, ok := sendStatusByClass[c]; ok {
				status, msg = s, sendMessageByClass[c]
			}
		}
	}
	resp.Error = msg
	writeJSON(w, status, resp)
}
