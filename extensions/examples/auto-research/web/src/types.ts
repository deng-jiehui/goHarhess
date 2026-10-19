export interface ResearchSettings {
  year_from: number;
  year_to: number;
  providers: string[];
  candidate_limit: number;
  analyze_limit: number;
}

export interface Project {
  id: string;
  owner_id: string;
  title: string;
  question: string;
  field: string;
  settings: ResearchSettings;
  archived: boolean;
  revision: number;
}

export interface Session {
  id: string;
  project_id: string;
  title: string;
  archived: boolean;
}

export interface Turn {
  id: string;
  session_id: string;
  project_id: string;
  state: string;
  input_text: string;
  revision: number;
  pending_questions_json?: string;
  error_code?: string;
  error_message?: string;
}

export interface MessageRecord {
  id: string;
  turn_id?: string;
  attempt?: number;
  content_json: string;
  committed: boolean;
}

export interface SessionEvent {
  id: number;
  session_id?: string;
  turn_id?: string;
  attempt?: number;
  kind: string;
  payload_json: string;
  stream_run_id?: string;
  sequence?: number;
  source_checkpoint_id?: string;
}

export interface ResearchProgress {
  id: string;
  stage: string;
  status: string;
  title: string;
  summary: string;
  facts?: string[];
  next_actions?: string[];
}

export interface WorkspaceToolEvent {
  tool: string;
  call_id: string;
  status: "completed" | "failed";
  is_error: boolean;
  preview?: string;
}

export interface ProjectNote {
  id: string;
  text: string;
  created_at: string;
  source_session_id?: string;
  source_turn_id?: string;
}

export interface Paper {
  id: string;
  project_id: string;
  source_key: string;
  title: string;
  authors: string[];
  year: number;
  doi?: string;
  arxiv_id?: string;
  url?: string;
  abstract?: string;
  tags: string[];
  note: string;
  reading_state: string;
  revision: number;
}

export interface PaperCandidate {
  source_key?: string;
  title: string;
  url?: string;
  doi?: string;
  arxiv_id?: string;
  authors?: string[];
  year?: number;
  abstract?: string;
  provider?: string;
}

export interface SourceDocument {
  id: string;
  paper_id: string;
  artifact_id: string;
  text: string;
  sha256: string;
  locator: string;
}

export interface Evidence {
  id: string;
  project_id: string;
  paper_id: string;
  document_id: string;
  field: string;
  value: string;
  quote: string;
  locator: string;
  start_rune: number;
  end_rune: number;
}

export interface SessionSnapshot {
  turn: Turn;
  messages: MessageRecord[];
  notes: ProjectNote[];
  events: SessionEvent[];
}

export interface ModelSettings {
  mode: "fixture" | "remote";
  endpoint?: string;
  model?: string;
  configured: boolean;
}

export interface ModelSettingsUpdate {
  mode: "fixture" | "remote";
  endpoint: string;
  model: string;
  api_key: string;
}

export type ProjectUpdate = Pick<
  Project,
  "title" | "question" | "field" | "settings" | "revision"
>;
