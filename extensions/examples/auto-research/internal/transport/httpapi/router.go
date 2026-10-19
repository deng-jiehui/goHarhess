package httpapi

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
)

type api struct {
	projects *app.ProjectService
	sessions *app.SessionService
	turns    *app.TurnService
	papers   *app.PaperService
	academic *app.AcademicService
	evidence *app.EvidenceService
	logger   *slog.Logger
}

func NewHandler(projects *app.ProjectService, sessions *app.SessionService, store Pinger, token string, logger *slog.Logger) http.Handler {
	return newHandler(projects, sessions, nil, nil, nil, nil, store, token, logger)
}

// NewHandlerWithRuntime 启用阶段二的会话执行接口；保留 NewHandler 兼容
// 阶段一只读项目 API 的调用方和测试。
func NewHandlerWithRuntime(projects *app.ProjectService, sessions *app.SessionService, turns *app.TurnService, store Pinger, token string, logger *slog.Logger) http.Handler {
	return newHandler(projects, sessions, turns, nil, nil, nil, store, token, logger)
}

// NewHandlerWithRuntimeAndPapers 启用会话执行和项目论文库接口。
func NewHandlerWithRuntimeAndPapers(projects *app.ProjectService, sessions *app.SessionService, turns *app.TurnService, papers *app.PaperService, store Pinger, token string, logger *slog.Logger) http.Handler {
	return newHandler(projects, sessions, turns, papers, nil, nil, store, token, logger)
}

// NewHandlerWithAcademic 启用论文库和学术搜索接口。
func NewHandlerWithAcademic(projects *app.ProjectService, sessions *app.SessionService, turns *app.TurnService, papers *app.PaperService, academic *app.AcademicService, store Pinger, token string, logger *slog.Logger) http.Handler {
	return newHandler(projects, sessions, turns, papers, academic, nil, store, token, logger)
}

// NewHandlerWithAcademicAndEvidence 启用论文、学术搜索与证据接口。
func NewHandlerWithAcademicAndEvidence(projects *app.ProjectService, sessions *app.SessionService, turns *app.TurnService, papers *app.PaperService, academic *app.AcademicService, evidence *app.EvidenceService, store Pinger, token string, logger *slog.Logger) http.Handler {
	return newHandler(projects, sessions, turns, papers, academic, evidence, store, token, logger)
}

func newHandler(projects *app.ProjectService, sessions *app.SessionService, turns *app.TurnService, papers *app.PaperService, academic *app.AcademicService, evidence *app.EvidenceService, store Pinger, token string, logger *slog.Logger) http.Handler {
	a := &api{projects: projects, sessions: sessions, turns: turns, papers: papers, academic: academic, evidence: evidence, logger: logger}
	routes := http.NewServeMux()
	routes.HandleFunc("GET /api/projects", a.listProjects)
	routes.HandleFunc("POST /api/projects", a.createProject)
	routes.HandleFunc("GET /api/projects/{projectID}", a.getProject)
	routes.HandleFunc("PATCH /api/projects/{projectID}", a.updateProject)
	routes.HandleFunc("POST /api/projects/{projectID}/archive", a.archiveProject)
	routes.HandleFunc("DELETE /api/projects/{projectID}", a.deleteProject)
	routes.HandleFunc("GET /api/projects/{projectID}/sessions", a.listSessions)
	routes.HandleFunc("POST /api/projects/{projectID}/sessions", a.createSession)
	routes.HandleFunc("GET /api/projects/{projectID}/sessions/{sessionID}", a.getSession)
	routes.HandleFunc("PATCH /api/projects/{projectID}/sessions/{sessionID}", a.updateSession)
	routes.HandleFunc("DELETE /api/projects/{projectID}/sessions/{sessionID}", a.deleteSession)
	routes.HandleFunc("POST /api/projects/{projectID}/sessions/{sessionID}/messages", a.sendMessage)
	routes.HandleFunc("GET /api/projects/{projectID}/sessions/{sessionID}/snapshot", a.getSnapshot)
	routes.HandleFunc("GET /api/projects/{projectID}/sessions/{sessionID}/events", a.listEvents)
	routes.HandleFunc("GET /api/projects/{projectID}/sessions/{sessionID}/notes", a.listNotes)
	routes.HandleFunc("GET /api/projects/{projectID}/notes", a.listNotes)
	routes.HandleFunc("DELETE /api/projects/{projectID}/notes/{noteID}", a.deleteNote)
	routes.HandleFunc("GET /api/projects/{projectID}/papers", a.listPapers)
	routes.HandleFunc("POST /api/projects/{projectID}/papers", a.importPaper)
	routes.HandleFunc("GET /api/projects/{projectID}/papers/{paperID}", a.getPaper)
	routes.HandleFunc("PATCH /api/projects/{projectID}/papers/{paperID}", a.updatePaper)
	routes.HandleFunc("POST /api/projects/{projectID}/academic/search", a.searchAcademic)
	routes.HandleFunc("GET /api/projects/{projectID}/papers/{paperID}/documents", a.listPaperDocuments)
	routes.HandleFunc("POST /api/projects/{projectID}/papers/{paperID}/documents", a.savePaperDocument)
	routes.HandleFunc("GET /api/projects/{projectID}/papers/{paperID}/evidence", a.listPaperEvidence)
	routes.HandleFunc("POST /api/projects/{projectID}/papers/{paperID}/evidence", a.savePaperEvidence)
	routes.HandleFunc("GET /api/projects/{projectID}/sessions/{sessionID}/turns/{turnID}", a.getTurn)
	routes.HandleFunc("POST /api/projects/{projectID}/sessions/{sessionID}/turns/{turnID}/resume", a.resumeTurn)
	routes.HandleFunc("POST /api/projects/{projectID}/sessions/{sessionID}/turns/{turnID}/retry", a.retryTurn)
	routes.HandleFunc("POST /api/projects/{projectID}/sessions/{sessionID}/turns/{turnID}/cancel", a.cancelTurn)
	routes.HandleFunc("GET /api/runtime/model", a.getModelSettings)
	routes.HandleFunc("PUT /api/runtime/model", a.updateModelSettings)
	routes.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { writeProblem(w, 404, "not_found", "内容不存在") })
	root := http.NewServeMux()
	health := NewHealthHandler(store)
	root.Handle("GET /healthz", health)
	root.Handle("GET /readyz", health)
	root.Handle("/api/", Protect(token, routes))
	root.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { writeProblem(w, 404, "not_found", "内容不存在") })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", rand.Text())
		root.ServeHTTP(w, r)
	})
}

func scope(r *http.Request) domain.Scope {
	// 第一阶段只有本地身份。后续认证入口可以替换此处，身份始终来自可信上下文。
	return domain.Scope{UserID: "local-user", ProjectID: r.PathValue("projectID")}
}

func decode(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return fmt.Errorf("%w：JSON 字段或格式不正确", domain.ErrInvalid)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w：请求只能包含一个 JSON 对象", domain.ErrInvalid)
	}
	return nil
}

func archivedFilter(r *http.Request) (bool, error) {
	value := r.URL.Query().Get("archived")
	if value == "" {
		return false, nil
	}
	archived, err := strconv.ParseBool(value)
	if err != nil {
		return false, domain.ErrInvalid
	}
	return archived, nil
}

func (a *api) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrForbidden):
		writeProblem(w, 404, "not_found", "内容不存在")
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrBusy):
		writeProblem(w, 409, "conflict", "内容已更新，请刷新数据后重新确认")
	case errors.Is(err, domain.ErrInvalid):
		writeProblem(w, 422, "invalid", err.Error())
	case errors.Is(err, domain.ErrUnavailable):
		a.logger.Error("数据操作失败", "request_id", w.Header().Get("X-Request-ID"), "error", err)
		writeProblem(w, 503, "unavailable", "本地数据服务暂不可用，请稍后重试")
	default:
		a.logger.Error("请求失败", "request_id", w.Header().Get("X-Request-ID"), "error", err)
		writeProblem(w, 500, "internal", "操作未能完成，请查看服务日志")
	}
}
