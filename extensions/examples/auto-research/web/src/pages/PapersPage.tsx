import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { Link, useParams } from "react-router-dom";
import { api, errorText } from "../api/client";
import type { Evidence, Paper, PaperCandidate, Project, SourceDocument } from "../types";
import { EmptyState } from "../components/EmptyState";

export function PapersPage() {
  const { projectId = "" } = useParams();
  const [project, setProject] = useState<Project | null>(null);
  const [papers, setPapers] = useState<Paper[]>([]);
  const [reference, setReference] = useState("");
  const [title, setTitle] = useState("");
  const [note, setNote] = useState("");
  const [searchText, setSearchText] = useState("");
  const [candidates, setCandidates] = useState<PaperCandidate[]>([]);
  const [searching, setSearching] = useState(false);
  const [selected, setSelected] = useState<Paper | null>(null);
  const [documents, setDocuments] = useState<SourceDocument[]>([]);
  const [evidence, setEvidence] = useState<Evidence[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  async function load(signal?: AbortSignal) {
    const [current, list] = await Promise.all([api.project(projectId, signal), api.papers(projectId, "", signal)]);
    setProject(current);
    setPapers(list.items);
  }

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    load(controller.signal).catch((reason) => {
      if (!controller.signal.aborted) setError(errorText(reason));
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false);
    });
    return () => controller.abort();
  }, [projectId]);

  useEffect(() => {
    if (!selected) {
      setDocuments([]);
      setEvidence([]);
      return;
    }
    const controller = new AbortController();
    Promise.all([api.paperDocuments(projectId, selected.id), api.paperEvidence(projectId, selected.id)]).then(([docs, cites]) => {
      if (!controller.signal.aborted) {
        setDocuments(docs.items);
        setEvidence(cites.items);
      }
    }).catch((reason) => {
      if (!controller.signal.aborted) setError(errorText(reason));
    });
    return () => controller.abort();
  }, [projectId, selected?.id]);

  async function importPaper(event: FormEvent) {
    event.preventDefault();
    if (!reference.trim() || !title.trim() || saving) return;
    setSaving(true);
    setError("");
    try {
      const paper = await api.importPaper(projectId, { title: title.trim(), doi: reference.trim() });
      setPapers((current) => current.some((item) => item.id === paper.id) ? current : [paper, ...current]);
      setSelected(paper);
      setReference("");
      setTitle("");
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setSaving(false);
    }
  }

  async function saveNote() {
    if (!selected || saving) return;
    setSaving(true);
    setError("");
    try {
      const paper = await api.updatePaper(projectId, selected.id, { note, revision: selected.revision });
      setSelected(paper);
      setPapers((current) => current.map((item) => item.id === paper.id ? paper : item));
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setSaving(false);
    }
  }

  async function searchAcademic(event: FormEvent) {
    event.preventDefault();
    if (!searchText.trim() || searching) return;
    setSearching(true);
    setError("");
    try {
      const result = await api.searchAcademic(projectId, { text: searchText.trim(), limit: 10 });
      setCandidates(result.items);
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setSearching(false);
    }
  }

  async function saveCandidate(candidate: PaperCandidate) {
    if (saving) return;
    setSaving(true);
    setError("");
    try {
      const paper = await api.importPaper(projectId, candidate);
      setPapers((current) => current.some((item) => item.id === paper.id) ? current : [paper, ...current]);
      setSelected(paper);
      setNote(paper.note);
    } catch (reason) {
      setError(errorText(reason));
    } finally {
      setSaving(false);
    }
  }

  if (loading) return <main className="standalone"><EmptyState loading title="正在打开论文库" description="正在读取项目资料…" /></main>;
  if (!project) return <main className="standalone"><EmptyState title="论文库不可用" description={error || "项目不存在。"} home /></main>;
  return (
    <main className="papers-page">
      <header className="papers-header">
        <div><Link to={`/p/${project.id}`} className="text-button">← 返回项目</Link><span className="eyebrow">PAPER LIBRARY</span><h1>{project.title} · 论文库</h1><p>先归档资料，再在会话里让 Agent 使用它们。</p></div>
      </header>
      {error && <p className="action-error" role="alert">{error}</p>}
      <section className="paper-import card">
        <h2>归档一篇论文</h2>
        <p className="muted">当前支持 DOI 或 arXiv ID。搜索工具接入后，候选也会从这里归档。</p>
        <form onSubmit={importPaper} className="paper-import-form">
          <input aria-label="论文标识" placeholder="例如 10.1234/example 或 2401.00001" value={reference} onChange={(event) => setReference(event.target.value)} />
          <input aria-label="论文标题" placeholder="论文标题" value={title} onChange={(event) => setTitle(event.target.value)} />
          <button className="button primary" disabled={saving || !reference.trim() || !title.trim()}>归档</button>
        </form>
      </section>
      <section className="paper-import card">
        <h2>搜索学术来源</h2>
        <p className="muted">当前使用离线 fixture 验证流程；接入 MCP 后沿用同一搜索接口。</p>
        <form onSubmit={searchAcademic} className="paper-import-form"><input aria-label="搜索论文" placeholder="例如 reproducible" value={searchText} onChange={(event) => setSearchText(event.target.value)} /><button className="button secondary" disabled={searching || !searchText.trim()}>搜索</button></form>
        {candidates.length > 0 && <div className="candidate-list">{candidates.map((candidate) => <div className="candidate-item" key={candidate.source_key}><div><strong>{candidate.title}</strong><small>{candidate.source_key}{candidate.year ? ` · ${candidate.year}` : ""}</small></div><button className="button secondary" disabled={saving} onClick={() => saveCandidate(candidate)}>归档到项目</button></div>)}</div>}
      </section>
      <div className="papers-layout">
        <section className="paper-list card"><div className="section-heading"><h2>已归档论文 <span>{papers.length}</span></h2></div>{papers.length === 0 ? <p className="muted">还没有归档论文。</p> : papers.map((paper) => <button className={`paper-list-item ${selected?.id === paper.id ? "active" : ""}`} key={paper.id} onClick={() => { setSelected(paper); setNote(paper.note); }}><strong>{paper.title}</strong><small>{paper.source_key}{paper.year ? ` · ${paper.year}` : ""}</small></button>)}</section>
        {selected ? <section className="paper-detail card"><span className="eyebrow">{selected.source_key}</span><h2>{selected.title}</h2><p className="muted">{selected.authors.join("、") || "作者待补充"}{selected.year ? ` · ${selected.year}` : ""}</p><div className="paper-abstract"><strong>摘要</strong><p>{selected.abstract || "当前只保存了元数据，尚未归档摘要或正文。"}</p></div><label className="paper-note-label">项目笔记<textarea value={note} onChange={(event) => setNote(event.target.value)} placeholder="记录这篇论文与你项目的关系…" /></label><button className="button primary" onClick={saveNote} disabled={saving || note === selected.note}>保存笔记</button><div className="paper-evidence"><h3>已归档来源与引用</h3>{documents.length === 0 ? <p className="muted">尚未归档可定位的来源文本。</p> : documents.map((document) => <details key={document.id}><summary>{document.locator || "来源片段"}</summary><p>{document.text}</p></details>)}{evidence.map((item) => <blockquote key={item.id}>“{item.quote}”<small>{item.locator}{item.field ? ` · ${item.field}` : ""}</small></blockquote>)}</div></section> : <section className="paper-detail card"><p className="muted">选择一篇论文查看详情。</p></section>}
      </div>
    </main>
  );
}
