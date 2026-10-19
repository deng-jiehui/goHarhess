import { createServer } from "node:http";
import type { Server } from "node:http";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { api, apiRequest, APIError } from "./client";

describe("HTTP 客户端边界", () => {
  let server: Server;
  let base: string;
  beforeAll(async () => {
    server = createServer((request, response) => {
      if (request.url === "/conflict") {
        response.writeHead(409, { "Content-Type": "application/json" });
        response.end(
          JSON.stringify({
            code: "conflict",
            message: "内容已更新",
            request_id: "request-123",
          }),
        );
      } else if (request.url === "/deleted") {
        response.writeHead(204);
        response.end();
      } else {
        response.writeHead(502, { "Content-Type": "text/html" });
        response.end("<html>gateway error</html>");
      }
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    if (!address || typeof address === "string")
      throw new Error("未获取测试端口");
    base = `http://127.0.0.1:${address.port}`;
  });
  afterAll(async () => {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("冲突保留错误身份供页面决定刷新而不是清空表单", async () => {
    try {
      await apiRequest(`${base}/conflict`);
      throw new Error("409 被当成成功");
    } catch (error) {
      expect(error).toBeInstanceOf(APIError);
      expect(error).toMatchObject({
        status: 409,
        code: "conflict",
        message: "内容已更新",
        requestID: "request-123",
      });
    }
  });
  it("删除的空响应不会引发 JSON 解析失败", async () => {
    await expect(
      apiRequest(`${base}/deleted`, { method: "DELETE" }),
    ).resolves.toBeUndefined();
  });
  it("网关 HTML 不会作为页面内容或成功结果返回", async () => {
    await expect(apiRequest(`${base}/gateway`)).rejects.toMatchObject({
      status: 502,
      code: "invalid_response",
    });
  });

  it("模型设置只提交配置字段并读取脱敏状态", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ mode: "remote", endpoint: "http://127.0.0.1:19090/v1", model: "test-model", configured: true }), { status: 200, headers: { "Content-Type": "application/json" } }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ mode: "remote", endpoint: "http://127.0.0.1:19090/v1", model: "test-model", configured: true }), { status: 200, headers: { "Content-Type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);
    await expect(api.modelSettings()).resolves.toMatchObject({ mode: "remote", configured: true });
    await expect(api.updateModelSettings({ mode: "remote", endpoint: "http://127.0.0.1:19090/v1", model: "test-model", api_key: "secret-key" })).resolves.toMatchObject({ configured: true });
    expect(fetchMock.mock.calls[1][0]).toBe("/api/runtime/model");
    expect(JSON.parse((fetchMock.mock.calls[1][1] as RequestInit).body as string)).toEqual({ mode: "remote", endpoint: "http://127.0.0.1:19090/v1", model: "test-model", api_key: "secret-key" });
    vi.unstubAllGlobals();
  });

  it("重发使用原 Turn 的 retry 接口", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ turn: { id: "turn-1" }, replayed: false }), {
        status: 202,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);
    await api.retryTurn("project-1", "session-1", {
      id: "turn-1",
      revision: 3,
    } as never);
    expect(fetchMock.mock.calls[0][0]).toBe(
      "/api/projects/project-1/sessions/session-1/turns/turn-1/retry",
    );
    expect(JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string)).toMatchObject({ revision: 3 });
    vi.unstubAllGlobals();
  });

  it("删除会话不需要提交会话名称", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    await expect(api.deleteSession("project-1", "session-1")).resolves.toBeUndefined();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/projects/project-1/sessions/session-1");
    expect(fetchMock.mock.calls[0][1]).toMatchObject({ method: "DELETE" });
    expect((fetchMock.mock.calls[0][1] as RequestInit).body).toBeUndefined();
    vi.unstubAllGlobals();
  });
});
