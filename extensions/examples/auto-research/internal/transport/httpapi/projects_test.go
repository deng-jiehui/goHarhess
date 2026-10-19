package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/goharness/autoresearch/internal/app"
	"github.com/goharness/autoresearch/internal/domain"
	"github.com/goharness/autoresearch/internal/infra/sqlite"
	"github.com/goharness/autoresearch/internal/transport/httpapi"
)

func testAPI(t *testing.T) (http.Handler, *sqlite.Store) {
	t.Helper()
	store, err := sqlite.Open(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return httpapi.NewHandler(app.NewProjectService(store), app.NewSessionService(store), store, "", slog.New(slog.NewTextHandler(io.Discard, nil))), store
}

func call(t *testing.T, handler http.Handler, method, path string, data any, want int) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	if data != nil {
		if err := json.NewEncoder(&body).Encode(data); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &body)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != want {
		t.Fatalf("%s %s = %d, want %d; %s", method, path, rr.Code, want, rr.Body.String())
	}
	return rr
}

func createProject(t *testing.T, handler http.Handler, title string) domain.Project {
	t.Helper()
	rr := call(t, handler, "POST", "/api/projects", map[string]any{"title": title}, 201)
	var project domain.Project
	if err := json.Unmarshal(rr.Body.Bytes(), &project); err != nil {
		t.Fatal(err)
	}
	if project.ID == "" {
		t.Fatal("未返回真实项目 ID")
	}
	return project
}

func TestProjectSettingsValidationIsAtomicAndStrict(t *testing.T) {
	h, _ := testAPI(t)
	p := createProject(t, h, "研究项目")
	path := "/api/projects/" + p.ID
	for _, bad := range []any{
		map[string]any{"title": "", "revision": 1},
		map[string]any{"owner_id": "someone", "revision": 1},
		map[string]any{"title": "不应保存", "revision": 1, "settings": map[string]any{"providers": []string{"arxiv"}, "candidate_limit": 5, "analyze_limit": 12}},
		map[string]any{"revision": 1, "settings": map[string]any{"providers": []string{"arxiv"}, "candidate_limit": 30, "analyze_limit": 12, "unknown": true}},
		map[string]any{"revision": 1, "settings": map[string]any{"year_from": 2026, "year_to": 2021, "providers": []string{"arxiv"}, "candidate_limit": 30, "analyze_limit": 12}},
	} {
		call(t, h, "PATCH", path, bad, 422)
	}
	rr := call(t, h, "GET", path, nil, 200)
	var saved domain.Project
	json.Unmarshal(rr.Body.Bytes(), &saved)
	if saved.Title != "研究项目" || saved.Revision != 1 {
		t.Fatalf("非法更新部分写入: %+v", saved)
	}
	call(t, h, "PATCH", path, map[string]any{"revision": 1, "question": "工具调用如何评测？", "field": "软件工程", "settings": map[string]any{"year_from": 2021, "year_to": 2026, "providers": []string{"arxiv", "arxiv"}, "candidate_limit": 30, "analyze_limit": 12}}, 200)
	rr = call(t, h, "GET", path, nil, 200)
	json.Unmarshal(rr.Body.Bytes(), &saved)
	if saved.Revision != 2 || saved.Question != "工具调用如何评测？" || len(saved.Settings.Providers) != 1 || saved.Settings.YearFrom != 2021 {
		t.Fatalf("设置未保存: %+v", saved)
	}
}

func TestConcurrentProjectEditsRejectStaleRevision(t *testing.T) {
	h, _ := testAPI(t)
	p := createProject(t, h, "并发修改")
	codes := make(chan int, 2)
	var wg sync.WaitGroup
	for _, title := range []string{"版本甲", "版本乙"} {
		wg.Add(1)
		go func(title string) {
			defer wg.Done()
			body, _ := json.Marshal(map[string]any{"revision": 1, "title": title})
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest("PATCH", "/api/projects/"+p.ID, bytes.NewReader(body)))
			codes <- rr.Code
		}(title)
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for code := range codes {
		counts[code]++
	}
	if counts[200] != 1 || counts[409] != 1 {
		t.Fatalf("旧版本更新未互斥: %v", counts)
	}
}

func TestArchiveRestoreAndConfirmedDeletion(t *testing.T) {
	h, _ := testAPI(t)
	p := createProject(t, h, "生命周期")
	path := "/api/projects/" + p.ID
	call(t, h, "POST", path+"/sessions", map[string]any{"title": "阅读"}, 201)
	call(t, h, "POST", path+"/archive", map[string]any{"revision": 1, "archived": true}, 200)
	rr := call(t, h, "GET", "/api/projects", nil, 200)
	var list struct {
		Items []domain.Project `json:"items"`
	}
	json.Unmarshal(rr.Body.Bytes(), &list)
	if len(list.Items) != 0 {
		t.Fatal("已归档项目出现在活动列表")
	}
	rr = call(t, h, "GET", "/api/projects?archived=true", nil, 200)
	json.Unmarshal(rr.Body.Bytes(), &list)
	if len(list.Items) != 1 {
		t.Fatal("归档项目无法查询")
	}
	call(t, h, "POST", path+"/archive", map[string]any{"revision": 2, "archived": false}, 200)
	call(t, h, "DELETE", path, map[string]any{"revision": 3, "confirm_title": "另一个项目"}, 422)
	call(t, h, "DELETE", path, map[string]any{"revision": 1, "confirm_title": "生命周期"}, 409)
	call(t, h, "GET", path, nil, 200)
	call(t, h, "DELETE", path, map[string]any{"revision": 3, "confirm_title": "生命周期"}, 204)
	call(t, h, "GET", path, nil, 404)
}

func TestSessionDeletionWithoutTitleConfirmationRemovesSession(t *testing.T) {
	h, _ := testAPI(t)
	project := createProject(t, h, "会话删除")
	path := "/api/projects/" + project.ID + "/sessions"
	rr := call(t, h, "POST", path, map[string]any{"title": "待删除会话"}, 201)
	var session domain.Session
	if err := json.Unmarshal(rr.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	sessionPath := path + "/" + session.ID
	call(t, h, "DELETE", sessionPath, nil, 204)
	call(t, h, "GET", sessionPath, nil, 404)
}

func TestAPIHidesForeignProjectAndRejectsForeignSession(t *testing.T) {
	h, store := testAPI(t)
	foreign, err := store.CreateProject(context.Background(), "another-user", "保密项目")
	if err != nil {
		t.Fatal(err)
	}
	call(t, h, "GET", "/api/projects/"+foreign.ID, nil, 404)
	p1 := createProject(t, h, "第一项目")
	p2 := createProject(t, h, "第二项目")
	rr := call(t, h, "POST", "/api/projects/"+p1.ID+"/sessions", map[string]any{"title": "论文阅读"}, 201)
	var session domain.Session
	json.Unmarshal(rr.Body.Bytes(), &session)
	wrong := "/api/projects/" + p2.ID + "/sessions/" + session.ID
	call(t, h, "GET", wrong, nil, 404)
	call(t, h, "PATCH", wrong, map[string]any{"title": "覆盖"}, 404)
	call(t, h, "PATCH", "/api/projects/"+p1.ID+"/sessions/"+session.ID, map[string]any{"title": "阅读笔记", "archived": true}, 200)
	rr = call(t, h, "GET", "/api/projects/"+p1.ID+"/sessions?archived=true", nil, 200)
	if !bytes.Contains(rr.Body.Bytes(), []byte("阅读笔记")) {
		t.Fatal("会话更新未保存")
	}
}

func TestAPIRejectsTrailingJSONAndUnavailableStore(t *testing.T) {
	h, store := testAPI(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "/api/projects", bytes.NewBufferString(`{"title":"第一个"}{"title":"第二个"}`)))
	if rr.Code != 422 {
		t.Fatalf("接受了额外 JSON: %d", rr.Code)
	}
	store.Close()
	rr = call(t, h, "GET", "/api/projects", nil, 503)
	var problem httpapi.Problem
	json.Unmarshal(rr.Body.Bytes(), &problem)
	if problem.Code != "unavailable" || problem.RequestID == "" || bytes.Contains(rr.Body.Bytes(), []byte("sql:")) {
		t.Fatalf("错误响应泄漏或缺字段: %s", rr.Body.String())
	}
}
