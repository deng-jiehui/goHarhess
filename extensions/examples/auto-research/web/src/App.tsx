import { BrowserRouter, Route, Routes } from "react-router-dom";
import { Notifications } from "./components/Notifications";
import { ProjectsPage } from "./pages/ProjectsPage";
import { WorkspacePage } from "./pages/WorkspacePage";
import { EmptyState } from "./components/EmptyState";
import { PapersPage } from "./pages/PapersPage";

export default function App() {
  return (
    <BrowserRouter>
      <Notifications>
        <Routes>
          <Route path="/" element={<ProjectsPage />} />
          <Route path="/p/:projectId" element={<WorkspacePage />} />
          <Route path="/p/:projectId/papers" element={<PapersPage />} />
          <Route
            path="/p/:projectId/s/:sessionId"
            element={<WorkspacePage />}
          />
          <Route
            path="*"
            element={
              <main className="standalone">
                <EmptyState
                  title="内容不存在"
                  description="这个地址没有对应的研究内容。"
                  home
                />
              </main>
            }
          />
        </Routes>
      </Notifications>
    </BrowserRouter>
  );
}
