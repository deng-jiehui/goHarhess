package domain

import "context"

type ProjectRepository interface {
	CreateProject(context.Context, string, string) (Project, error)
	GetProject(context.Context, Scope) (Project, error)
	ListProjects(context.Context, string) ([]Project, error)
	UpdateProject(context.Context, Scope, Project, int64) error
	DeleteProject(context.Context, Scope, string, int64) error
}

type SessionRepository interface {
	CreateSession(context.Context, Scope, string) (Session, error)
	GetSession(context.Context, Scope, string) (Session, error)
	ListSessions(context.Context, Scope) ([]Session, error)
	UpdateSession(context.Context, Scope, Session) error
	DeleteSession(context.Context, Scope, string) error
}

type PaperRepository interface {
	ImportPaper(context.Context, Scope, Paper) (Paper, error)
	GetPaper(context.Context, Scope, string) (Paper, error)
	GetPaperBySourceKey(context.Context, Scope, string) (Paper, error)
	ListPapers(context.Context, Scope, PaperQuery) ([]Paper, error)
	UpdatePaper(context.Context, Scope, Paper, int64) error
}

type EvidenceRepository interface {
	SaveDocument(context.Context, Scope, SourceDocument) (SourceDocument, error)
	GetDocument(context.Context, Scope, string) (SourceDocument, error)
	ListDocuments(context.Context, Scope, string) ([]SourceDocument, error)
	SaveEvidence(context.Context, Scope, Evidence) (Evidence, error)
	ListEvidence(context.Context, Scope, string) ([]Evidence, error)
}

type TurnDraft struct {
	ID                string
	InputText         string
	IdempotencyKey    string
	DefinitionVersion string
	ContextJSON       []byte
}

type AdmitResult struct {
	Turn     Turn
	Replayed bool
}

type ActionDraft struct {
	ID             string
	Kind           string
	IdempotencyKey string
	RequestHash    string
	PayloadJSON    []byte
	Revision       int64
}

type ActionResult struct {
	Turn     Turn
	Action   TurnAction
	Replayed bool
}

type RuntimeRepository interface {
	AdmitTurn(context.Context, Scope, string, TurnDraft) (AdmitResult, error)
	GetTurn(context.Context, Scope, string, string) (Turn, error)
	ListTurns(context.Context, Scope, string, int) ([]Turn, error)
	ClaimQueued(context.Context) (Turn, bool, error)
	SetTurnState(context.Context, Scope, string, string, string, string) error
	SetPendingQuestions(context.Context, Scope, string, []byte) error
	ApplyAction(context.Context, Scope, string, string, ActionDraft) (ActionResult, error)
	ListActions(context.Context, Scope, string, string) ([]TurnAction, error)
	SaveMessages(context.Context, Scope, string, int, []MessageRecord) error
	ListMessages(context.Context, Scope, string, int) ([]MessageRecord, error)
	SaveNote(context.Context, Scope, Note) (Note, error)
	ListNotes(context.Context, Scope, int) ([]Note, error)
	DeleteNote(context.Context, Scope, string) error
	AppendEvent(context.Context, Scope, SessionEvent) (SessionEvent, error)
	ListEvents(context.Context, Scope, string, int64, int) ([]SessionEvent, error)
	Snapshot(context.Context, Scope, string, int) (TurnSnapshot, error)
}
