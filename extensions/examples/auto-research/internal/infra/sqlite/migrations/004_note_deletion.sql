-- 保留保存调用的幂等身份，避免旧轮重试后重新显示用户已删除的笔记。
ALTER TABLE notes ADD COLUMN deleted_at TEXT;
CREATE INDEX notes_active_project ON notes(project_id, owner_id, deleted_at);
