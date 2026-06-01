package riverworkflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

const (
	MetadataKeyWorkflowID   = "workflow_id"
	MetadataKeyWorkflowName = "workflow_name"
	MetadataKeyWorkflowTask = "workflow_task"
	MetadataKeyWorkflowDeps = "workflow_deps"
	MetadataKeyWorkflowWait = "workflow_wait"
)

var celIdentRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func metadataMap(metadata []byte) map[string]json.RawMessage {
	if len(metadata) == 0 {
		return nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &m); err != nil {
		return nil
	}
	return m
}

func stringFromMetadata(metadata []byte, key string) string {
	m := metadataMap(metadata)
	if m == nil {
		return ""
	}
	var s string
	_ = json.Unmarshal(m[key], &s)
	return s
}

func IDFromJobRow(job *rivertype.JobRow) string {
	if job == nil {
		return ""
	}
	return IDFromMetadata(job.Metadata)
}
func IDFromMetadata(metadata []byte) string {
	return stringFromMetadata(metadata, MetadataKeyWorkflowID)
}
func NameFromJobRow(job *rivertype.JobRow) string {
	if job == nil {
		return ""
	}
	return NameFromMetadata(job.Metadata)
}
func NameFromMetadata(metadata []byte) string {
	return stringFromMetadata(metadata, MetadataKeyWorkflowName)
}
func TaskFromJobRow(job *rivertype.JobRow) string {
	if job == nil {
		return ""
	}
	return TaskFromMetadata(job.Metadata)
}
func TaskFromMetadata(metadata []byte) string {
	return stringFromMetadata(metadata, MetadataKeyWorkflowTask)
}
func DepsFromJobRow(job *rivertype.JobRow) []string {
	if job == nil {
		return nil
	}
	return DepsFromMetadata(job.Metadata)
}
func DepsFromMetadata(metadata []byte) []string {
	m := metadataMap(metadata)
	if m == nil {
		return nil
	}
	var deps []string
	_ = json.Unmarshal(m[MetadataKeyWorkflowDeps], &deps)
	return deps
}

func JobListParams(job *rivertype.JobRow, params *river.JobListParams) (*river.JobListParams, error) {
	workflowID := IDFromJobRow(job)
	if workflowID == "" {
		return nil, errors.New("riverworkflow: job is not part of a workflow")
	}
	return JobListParamsByID(workflowID, params)
}

func JobListParamsByID(workflowID string, params *river.JobListParams) (*river.JobListParams, error) {
	if workflowID == "" {
		return nil, errors.New("riverworkflow: workflow ID is empty")
	}
	if params == nil {
		params = river.NewJobListParams()
	}
	fragment, _ := json.Marshal(map[string]string{MetadataKeyWorkflowID: workflowID})
	return params.Metadata(string(fragment)), nil
}

type Signal struct {
	Attempt    int             `json:"attempt"`
	CreatedAt  time.Time       `json:"created_at"`
	ID         int64           `json:"id"`
	Key        string          `json:"key"`
	Payload    json.RawMessage `json:"payload"`
	Source     json.RawMessage `json:"source"`
	WorkflowID string          `json:"workflow_id"`
}

type SignalAttemptMismatchError struct {
	RequestedAttempt, SignalAttempt int
	WorkflowID                      string
}

func (e *SignalAttemptMismatchError) Error() string {
	return fmt.Sprintf("riverworkflow: signal attempt mismatch for workflow %q: requested %d, got %d", e.WorkflowID, e.RequestedAttempt, e.SignalAttempt)
}
func (e *SignalAttemptMismatchError) Is(target error) bool {
	_, ok := target.(*SignalAttemptMismatchError)
	return ok
}

type SignalKeyUndeclaredError struct{ Key, TaskName, WorkflowID string }

func (e *SignalKeyUndeclaredError) Error() string {
	return fmt.Sprintf("riverworkflow: signal key %q is not declared by task %q in workflow %q", e.Key, e.TaskName, e.WorkflowID)
}
func (e *SignalKeyUndeclaredError) Is(target error) bool {
	_, ok := target.(*SignalKeyUndeclaredError)
	return ok
}

type SignalPayloadMismatchError struct {
	SignalID   *int64
	WorkflowID string
}

func (e *SignalPayloadMismatchError) Error() string {
	return fmt.Sprintf("riverworkflow: signal payload mismatch in workflow %q", e.WorkflowID)
}
func (e *SignalPayloadMismatchError) Is(target error) bool {
	_, ok := target.(*SignalPayloadMismatchError)
	return ok
}

type SignalTaskDeclaresNoSignalKeysError struct{ TaskName, WorkflowID string }

func (e *SignalTaskDeclaresNoSignalKeysError) Error() string {
	return fmt.Sprintf("riverworkflow: task %q in workflow %q declares no signal keys", e.TaskName, e.WorkflowID)
}
func (e *SignalTaskDeclaresNoSignalKeysError) Is(target error) bool {
	_, ok := target.(*SignalTaskDeclaresNoSignalKeysError)
	return ok
}

type SignalUnknownTaskError struct{ TaskName, WorkflowID string }

func (e *SignalUnknownTaskError) Error() string {
	return fmt.Sprintf("riverworkflow: unknown task %q in workflow %q", e.TaskName, e.WorkflowID)
}
func (e *SignalUnknownTaskError) Is(target error) bool {
	_, ok := target.(*SignalUnknownTaskError)
	return ok
}

type TimerAnchorKind string

const (
	TimerAnchorKindTaskFinalizedAt   TimerAnchorKind = "task_finalized_at"
	TimerAnchorKindWaitStartedAt     TimerAnchorKind = "wait_started_at"
	TimerAnchorKindWorkflowCreatedAt TimerAnchorKind = "workflow_created_at"
)

type TimerAnchor struct {
	Kind     TimerAnchorKind `json:"kind,omitempty"`
	TaskName string          `json:"task_name,omitempty"`
}

type Timer struct {
	name   string
	fireAt *time.Time
	after  *time.Duration
	anchor TimerAnchor
}

func TimerAt(name string, fireAt time.Time) Timer { return Timer{name: name, fireAt: &fireAt} }
func TimerAfterWaitStarted(name string, after time.Duration) Timer {
	return Timer{name: name, after: &after, anchor: TimerAnchor{Kind: TimerAnchorKindWaitStartedAt}}
}
func TimerAfterWorkflowCreated(name string, after time.Duration) Timer {
	return Timer{name: name, after: &after, anchor: TimerAnchor{Kind: TimerAnchorKindWorkflowCreatedAt}}
}
func TimerAfterTaskFinalized(name string, taskName string, after time.Duration) Timer {
	return Timer{name: name, after: &after, anchor: TimerAnchor{Kind: TimerAnchorKindTaskFinalizedAt, TaskName: taskName}}
}
func (t Timer) Name() string { return t.name }
func (t Timer) FireAt() (time.Time, bool) {
	if t.fireAt == nil {
		return time.Time{}, false
	}
	return *t.fireAt, true
}
func (t Timer) After() (time.Duration, bool) {
	if t.after == nil {
		return 0, false
	}
	return *t.after, true
}
func (t Timer) Anchor() TimerAnchor { return t.anchor }

func (t Timer) MarshalJSON() ([]byte, error) {
	m := map[string]any{"name": t.name}
	if t.fireAt != nil {
		m["fire_at"] = t.fireAt
	}
	if t.after != nil {
		m["after"] = t.after.String()
		m["after_ns"] = int64(*t.after)
		m["anchor"] = t.anchor
	}
	return json.Marshal(m)
}
func (t *Timer) UnmarshalJSON(data []byte) error {
	var m struct {
		Name    string      `json:"name"`
		FireAt  *time.Time  `json:"fire_at"`
		After   string      `json:"after"`
		AfterNS *int64      `json:"after_ns"`
		Anchor  TimerAnchor `json:"anchor"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	t.name, t.fireAt, t.anchor = m.Name, m.FireAt, m.Anchor
	if m.AfterNS != nil {
		d := time.Duration(*m.AfterNS)
		t.after = &d
	} else if m.After != "" {
		d, err := time.ParseDuration(m.After)
		if err != nil {
			return err
		}
		t.after = &d
	}
	return nil
}

type Wait struct {
	Evidence   *WaitEvidence    `json:"evidence,omitempty"`
	Expr       string           `json:"expr,omitempty"`
	Inputs     WaitInputState   `json:"inputs,omitempty"`
	Phase      WaitPhase        `json:"phase"`
	ResolvedAt *time.Time       `json:"resolved_at,omitempty"`
	StartedAt  *time.Time       `json:"started_at,omitempty"`
	Summary    string           `json:"summary,omitempty"`
	Terms      []WaitTermStatus `json:"terms,omitempty"`
}

func WaitFromMetadata(metadata []byte) (*Wait, error) {
	m := metadataMap(metadata)
	if m == nil || len(m[MetadataKeyWorkflowWait]) == 0 {
		return nil, nil
	}
	raw := m[MetadataKeyWorkflowWait]
	if !bytes.Contains(raw, []byte(`"phase"`)) {
		var spec WaitSpec
		if err := json.Unmarshal(raw, &spec); err != nil {
			return nil, &WaitMetadataDecodeError{Field: MetadataKeyWorkflowWait, Err: err}
		}
		return spec.Status(), nil
	}
	var w Wait
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, &WaitMetadataDecodeError{Field: MetadataKeyWorkflowWait, Err: err}
	}
	return &w, nil
}
func (w *Wait) DepInput(taskName string) *WaitDepInput {
	if w == nil {
		return nil
	}
	for i := range w.Inputs.Deps {
		if w.Inputs.Deps[i].TaskName == taskName {
			return &w.Inputs.Deps[i]
		}
	}
	return nil
}
func (w *Wait) SignalInput(key string) *WaitSignalInput {
	if w == nil {
		return nil
	}
	for i := range w.Inputs.Signals {
		if w.Inputs.Signals[i].Key == key {
			return &w.Inputs.Signals[i]
		}
	}
	return nil
}
func (w *Wait) TimerInput(name string) *WaitTimerInput {
	if w == nil {
		return nil
	}
	for i := range w.Inputs.Timers {
		if w.Inputs.Timers[i].Name == name {
			return &w.Inputs.Timers[i]
		}
	}
	return nil
}
func (w *Wait) Term(name string) *WaitTermStatus {
	if w == nil {
		return nil
	}
	for i := range w.Terms {
		if w.Terms[i].Name == name {
			return &w.Terms[i]
		}
	}
	return nil
}

type WaitEvidence struct {
	EvaluatedAt     time.Time `json:"evaluated_at"`
	WorkflowAttempt int       `json:"workflow_attempt"`
}
type WaitInputState struct {
	Deps    []WaitDepInput    `json:"deps,omitempty"`
	Signals []WaitSignalInput `json:"signals,omitempty"`
	Timers  []WaitTimerInput  `json:"timers,omitempty"`
}
type WaitDepInput struct {
	Result   *WaitDepInputResult `json:"result,omitempty"`
	TaskName string              `json:"task_name"`
}
type WaitDepInputResult struct {
	Available   bool       `json:"available"`
	FinalizedAt *time.Time `json:"finalized_at,omitempty"`
	State       string     `json:"state,omitempty"`
}
type WaitSignalInput struct {
	Key    string                 `json:"key"`
	Result *WaitSignalInputResult `json:"result,omitempty"`
}
type WaitSignalInputResult struct {
	IncludedCount  int64  `json:"included_count"`
	LastIncludedID *int64 `json:"last_included_id,omitempty"`
}
type WaitTimerInput struct {
	After  *time.Duration        `json:"after,omitempty"`
	Anchor *TimerAnchor          `json:"anchor,omitempty"`
	FireAt *time.Time            `json:"fire_at,omitempty"`
	Name   string                `json:"name"`
	Result *WaitTimerInputResult `json:"result,omitempty"`
}
type WaitTimerInputResult struct {
	FireAt *time.Time `json:"fire_at,omitempty"`
	Fired  bool       `json:"fired"`
}

type WaitDepDiagnostic struct {
	Available   bool
	FinalizedAt *time.Time
	State       string
	TaskName    string
}
type WaitSignalDiagnostic struct {
	IncludedCount int64
	Key           string
	LastID        *int64
}
type WaitTimerDiagnostic struct {
	FireAt *time.Time
	Fired  bool
	Name   string
}
type WaitTermDiagnostic struct {
	LastMatchedID *int64
	MatchedCount  int64
	Name          string
	RequiredCount int64
	Satisfied     bool
}
type WaitDiagnosticsInputs struct {
	Deps    []WaitDepDiagnostic
	Signals []WaitSignalDiagnostic
	Timers  []WaitTimerDiagnostic
}
type WaitDiagnostics struct {
	EvalError       error
	ExprResult      *bool
	Inputs          WaitDiagnosticsInputs
	InspectedAt     time.Time
	Phase           WaitPhase
	SignalScanCount int
	SignalScanLimit int
	Terms           []WaitTermDiagnostic
	Truncated       bool
	WorkflowAttempt int
}

type WaitInputs struct {
	Signals []string `json:"signals,omitempty"`
	Timers  []Timer  `json:"timers,omitempty"`
}

type WaitMetadataDecodeError struct {
	Err   error
	Field string
}

func (e *WaitMetadataDecodeError) Error() string {
	return fmt.Sprintf("riverworkflow: could not decode wait metadata field %q: %v", e.Field, e.Err)
}
func (e *WaitMetadataDecodeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type WaitPhase int

const (
	WaitPhaseNotStarted WaitPhase = iota
	WaitPhaseWaiting
	WaitPhaseResolved
)

func (p WaitPhase) String() string {
	switch p {
	case WaitPhaseNotStarted:
		return "not_started"
	case WaitPhaseWaiting:
		return "waiting"
	case WaitPhaseResolved:
		return "resolved"
	default:
		return "unknown"
	}
}

type WaitSpec struct {
	Expr   string         `json:"expr,omitempty"`
	Inputs WaitInputs     `json:"inputs,omitempty"`
	Terms  []WaitTermSpec `json:"terms,omitempty"`
}

func (w WaitSpec) MarshalJSON() ([]byte, error) { type alias WaitSpec; return json.Marshal(alias(w)) }
func (w *WaitSpec) UnmarshalJSON(data []byte) error {
	type alias WaitSpec
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*w = WaitSpec(a)
	return nil
}
func (w *WaitSpec) SignalKeys() []string {
	if w == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, k := range w.Inputs.Signals {
		if k != "" && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	for _, t := range w.Terms {
		if t.kind == WaitTermKindSignal && t.signalKey != "" && !seen[t.signalKey] {
			seen[t.signalKey] = true
			out = append(out, t.signalKey)
		}
	}
	return out
}
func (w *WaitSpec) Timers() []Timer {
	if w == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []Timer
	for _, t := range w.Inputs.Timers {
		if t.Name() != "" && !seen[t.Name()] {
			seen[t.Name()] = true
			out = append(out, t)
		}
	}
	for _, term := range w.Terms {
		if term.kind == WaitTermKindTimer && term.timer.Name() != "" && !seen[term.timer.Name()] {
			seen[term.timer.Name()] = true
			out = append(out, term.timer)
		}
	}
	return out
}
func (w *WaitSpec) Validate(deps []string) error {
	if w == nil {
		return nil
	}
	depSet := map[string]bool{}
	for _, d := range deps {
		depSet[d] = true
	}
	termNames := map[string]bool{}
	reserved := map[string]bool{"deps": true, "signals": true, "timers": true, "workflow": true, "true": true, "false": true, "null": true}
	for _, t := range w.Terms {
		if t.name == "" {
			return errors.New("wait term name is empty")
		}
		if !celIdentRE.MatchString(t.name) {
			return fmt.Errorf("wait term %q is not a valid CEL identifier", t.name)
		}
		if reserved[t.name] {
			return fmt.Errorf("wait term %q is reserved", t.name)
		}
		if termNames[t.name] {
			return fmt.Errorf("duplicate wait term %q", t.name)
		}
		termNames[t.name] = true
		switch t.kind {
		case WaitTermKindGeneric:
			if strings.TrimSpace(t.expr) == "" {
				return fmt.Errorf("wait term %q expression is empty", t.name)
			}
		case WaitTermKindSignal:
			if t.signalKey == "" {
				return fmt.Errorf("wait term %q signal key is empty", t.name)
			}
			if strings.TrimSpace(t.expr) == "" {
				return fmt.Errorf("wait term %q expression is empty", t.name)
			}
			if t.count < 0 {
				return fmt.Errorf("wait term %q count must be positive", t.name)
			}
		case WaitTermKindTimer:
			if t.timer.Name() == "" {
				return fmt.Errorf("timer term %q has empty timer name", t.name)
			}
			if t.timer.anchor.Kind == TimerAnchorKindTaskFinalizedAt && !depSet[t.timer.anchor.TaskName] {
				return fmt.Errorf("timer %q is anchored to non-dependency task %q", t.timer.Name(), t.timer.anchor.TaskName)
			}
		default:
			return fmt.Errorf("wait term %q has unknown kind %q", t.name, t.kind)
		}
	}
	seenSignals := map[string]bool{}
	for _, k := range w.SignalKeys() {
		if k == "" {
			return errors.New("signal key is empty")
		}
		if seenSignals[k] {
			return fmt.Errorf("duplicate signal key %q", k)
		}
		seenSignals[k] = true
	}
	seenTimers := map[string]bool{}
	for _, t := range w.Timers() {
		if t.Name() == "" {
			return errors.New("timer name is empty")
		}
		if seenTimers[t.Name()] {
			return fmt.Errorf("duplicate timer %q", t.Name())
		}
		seenTimers[t.Name()] = true
		if _, ok := t.FireAt(); !ok {
			if after, ok := t.After(); !ok || after <= 0 {
				return fmt.Errorf("timer %q must have a positive relative duration or absolute fire time", t.Name())
			}
		}
	}
	if w.Expr == "" && len(w.Terms) > 1 {
		names := make([]string, 0, len(w.Terms))
		for _, t := range w.Terms {
			names = append(names, t.name)
		}
		sort.Strings(names)
		_ = names
	}
	return nil
}
func (w *WaitSpec) Status() *Wait {
	if w == nil {
		return nil
	}
	wait := &Wait{Expr: w.Expr, Phase: WaitPhaseNotStarted}
	for _, d := range []string{} {
		wait.Inputs.Deps = append(wait.Inputs.Deps, WaitDepInput{TaskName: d})
	}
	for _, k := range w.SignalKeys() {
		wait.Inputs.Signals = append(wait.Inputs.Signals, WaitSignalInput{Key: k})
	}
	for _, t := range w.Timers() {
		ti := WaitTimerInput{Name: t.Name()}
		if after, ok := t.After(); ok {
			ti.After = &after
			a := t.Anchor()
			ti.Anchor = &a
		}
		if fireAt, ok := t.FireAt(); ok {
			ti.FireAt = &fireAt
		}
		wait.Inputs.Timers = append(wait.Inputs.Timers, ti)
	}
	for _, t := range w.Terms {
		wait.Terms = append(wait.Terms, t.status())
	}
	return wait
}

type WaitTaskDeclaresNoWaitError struct{ TaskName, WorkflowID string }

func (e *WaitTaskDeclaresNoWaitError) Error() string {
	return fmt.Sprintf("riverworkflow: task %q in workflow %q declares no wait", e.TaskName, e.WorkflowID)
}
func (e *WaitTaskDeclaresNoWaitError) Is(target error) bool {
	_, ok := target.(*WaitTaskDeclaresNoWaitError)
	return ok
}

type WaitTermKind string

const (
	WaitTermKindGeneric WaitTermKind = "generic"
	WaitTermKindSignal  WaitTermKind = "signal"
	WaitTermKindTimer   WaitTermKind = "timer"
)

type WaitTermSpec struct {
	name      string
	expr      string
	kind      WaitTermKind
	signalKey string
	timer     Timer
	count     int
	label     string
}

func WaitTerm(name, expr string) WaitTermSpec {
	return WaitTermSpec{name: name, expr: expr, kind: WaitTermKindGeneric}
}
func WaitTermSignal(name, signalKey, expr string) WaitTermSpec {
	return WaitTermSpec{name: name, signalKey: signalKey, expr: expr, kind: WaitTermKindSignal}
}
func WaitTermTimer(timer Timer) WaitTermSpec {
	return WaitTermSpec{name: timer.Name(), kind: WaitTermKindTimer, timer: timer}
}
func (t WaitTermSpec) Count(count int) WaitTermSpec { t.count = count; return t }
func (t WaitTermSpec) CountRequirement() int {
	if t.count > 0 {
		return t.count
	}
	if t.kind == WaitTermKindSignal {
		return 1
	}
	return 0
}
func (t WaitTermSpec) DisplayLabel() string {
	if t.label != "" {
		return t.label
	}
	if t.signalKey != "" {
		return t.signalKey
	}
	if t.timer.Name() != "" {
		return t.timer.Name()
	}
	return strings.ReplaceAll(t.name, "_", " ")
}
func (t WaitTermSpec) Expr() string                    { return t.expr }
func (t WaitTermSpec) Kind() WaitTermKind              { return t.kind }
func (t WaitTermSpec) Label(label string) WaitTermSpec { t.label = label; return t }
func (t WaitTermSpec) Name() string                    { return t.name }
func (t WaitTermSpec) SignalKey() string               { return t.signalKey }
func (t WaitTermSpec) Timer() Timer                    { return t.timer }
func (t WaitTermSpec) UserLabel() string               { return t.label }
func (t WaitTermSpec) status() WaitTermStatus {
	return WaitTermStatus{Expr: t.expr, Kind: t.kind, Label: t.label, Name: t.name, RequiredCount: int64(t.CountRequirement()), SignalKey: t.signalKey, TimerName: t.timer.Name()}
}
func (t WaitTermSpec) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"name": t.name, "expr": t.expr, "kind": t.kind, "signal_key": t.signalKey, "timer": t.timer, "count": t.count, "label": t.label})
}
func (t *WaitTermSpec) UnmarshalJSON(data []byte) error {
	var m struct {
		Name      string       `json:"name"`
		Expr      string       `json:"expr"`
		Kind      WaitTermKind `json:"kind"`
		SignalKey string       `json:"signal_key"`
		Timer     Timer        `json:"timer"`
		Count     int          `json:"count"`
		Label     string       `json:"label"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*t = WaitTermSpec{name: m.Name, expr: m.Expr, kind: m.Kind, signalKey: m.SignalKey, timer: m.Timer, count: m.Count, label: m.Label}
	return nil
}

type WaitTermStatus struct {
	Expr          string                `json:"expr,omitempty"`
	Kind          WaitTermKind          `json:"kind,omitempty"`
	Label         string                `json:"label,omitempty"`
	Name          string                `json:"name"`
	RequiredCount int64                 `json:"required_count,omitempty"`
	Result        *WaitTermStatusResult `json:"result,omitempty"`
	SignalKey     string                `json:"signal_key,omitempty"`
	TimerName     string                `json:"timer_name,omitempty"`
}
type WaitTermStatusResult struct {
	LastMatchedID *int64 `json:"last_matched_id,omitempty"`
	MatchedCount  int64  `json:"matched_count"`
	Satisfied     bool   `json:"satisfied"`
}

type WaitValidationError struct {
	Err                            error
	TaskName, WaitExpr, WorkflowID string
}

func (e *WaitValidationError) Error() string {
	return fmt.Sprintf("riverworkflow: wait validation failed for task %q workflow %q: %v", e.TaskName, e.WorkflowID, e.Err)
}
func (e *WaitValidationError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}
