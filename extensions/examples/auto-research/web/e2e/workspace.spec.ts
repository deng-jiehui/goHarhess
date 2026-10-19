import { test, expect } from "@playwright/test";
import type { Page, APIRequestContext } from "@playwright/test";

async function createProject(request: APIRequestContext, title: string) {
  const response = await request.post("/api/projects", { data: { title } });
  expect(response.status()).toBe(201);
  return response.json() as Promise<{ id: string; revision: number }>;
}

async function openSettings(page: Page) {
  await page.getByRole("button", { name: "项目设置", exact: true }).click();
  return page.getByRole("dialog", { name: "项目设置", exact: true });
}

test("创建项目和会话后刷新仍可继续", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "新建项目", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("项目名称", { exact: true }).fill("检索模型对比");
  await dialog.getByRole("button", { name: "创建项目", exact: true }).click();
  await page.getByRole("button", { name: "新建会话", exact: true }).click();
  await expect(page).toHaveURL(/\/s\/[^/]+$/);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "检索模型对比", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("navigation", { name: "会话列表" }),
  ).toContainText("新会话");
  await expect(page.getByRole("button", { name: "发送消息" })).toBeDisabled();
  await page.getByRole("button", { name: "重命名会话" }).click();
  await page.getByLabel("会话名称").fill("基线论文阅读");
  await page.getByRole("button", { name: "保存会话名称" }).click();
  await page.reload();
  await expect(
    page.getByRole("navigation", { name: "会话列表" }),
  ).toContainText("基线论文阅读");
});

test("研究设置持久保存且非法设置不会部分写入", async ({ page, request }) => {
  const project = await createProject(request, "设置验收");
  await page.goto(`/p/${project.id}`);
  let dialog = await openSettings(page);
  await dialog
    .getByLabel("研究问题")
    .fill("Agent 的工具选择如何影响修复成功率？");
  await dialog.getByLabel("研究领域").fill("软件工程");
  await dialog.getByLabel("起始年份").fill("2021");
  await dialog.getByLabel("结束年份").fill("2026");
  await dialog.getByRole("button", { name: "保存设置" }).click();
  await expect(dialog).not.toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("complementary", { name: "研究上下文" }),
  ).toContainText("2021–2026");
  dialog = await openSettings(page);
  await expect(dialog.getByLabel("研究问题")).toHaveValue(
    "Agent 的工具选择如何影响修复成功率？",
  );
  await dialog.getByLabel("项目名称", { exact: true }).fill("这次不应保存");
  await dialog.getByLabel("候选论文上限").fill("5");
  await dialog.getByRole("button", { name: "保存设置" }).click();
  await expect(dialog.getByRole("alert")).toBeVisible();
  await expect(dialog.getByLabel("项目名称", { exact: true })).toHaveValue(
    "这次不应保存",
  );
  const saved = await (await request.get(`/api/projects/${project.id}`)).json();
  expect(saved.title).toBe("设置验收");
});

test("版本冲突会保留正在编辑的表单", async ({ page, request }) => {
  const project = await createProject(request, "版本冲突验收");
  await page.goto(`/p/${project.id}`);
  const dialog = await openSettings(page);
  const response = await request.patch(`/api/projects/${project.id}`, {
    data: { title: "另一窗口的新名称", revision: 1 },
  });
  expect(response.status()).toBe(200);
  await dialog.getByLabel("研究问题").fill("我的编辑不能丢失");
  await dialog.getByRole("button", { name: "保存设置" }).click();
  await expect(dialog.getByRole("alert")).toContainText("已更新");
  await expect(dialog.getByLabel("研究问题")).toHaveValue("我的编辑不能丢失");
});

test("项目归档恢复和删除必须经过明确确认", async ({ page, request }) => {
  const project = await createProject(request, "归档删除验收");
  await page.goto(`/p/${project.id}`);
  let dialog = await openSettings(page);
  await dialog.getByRole("button", { name: "归档项目", exact: true }).click();
  await expect(page).toHaveURL("/");
  await page.getByRole("button", { name: "已归档", exact: true }).click();
  await page.getByRole("link", { name: "归档删除验收", exact: true }).click();
  dialog = await openSettings(page);
  await dialog.getByRole("button", { name: "恢复项目", exact: true }).click();
  await expect(page.getByText("项目已恢复", { exact: true })).toBeVisible();
  dialog = await openSettings(page);
  await dialog
    .getByRole("button", { name: "永久删除项目", exact: true })
    .click();
  const deletion = page.getByRole("dialog", { name: "删除项目", exact: true });
  await expect(
    deletion.getByRole("button", { name: "确认永久删除" }),
  ).toBeDisabled();
  await deletion.getByLabel("输入项目名称确认").fill("归档删除验收");
  await deletion.getByRole("button", { name: "确认永久删除" }).click();
  await expect(page).toHaveURL("/");
  expect((await request.get(`/api/projects/${project.id}`)).status()).toBe(404);
});

test("错误项目的会话地址显示不存在并适配窄屏", async ({ page, request }) => {
  const one = await createProject(request, "归属项目一");
  const two = await createProject(request, "归属项目二");
  const session = await (
    await request.post(`/api/projects/${one.id}/sessions`, {
      data: { title: "不能泄漏的会话" },
    })
  ).json();
  await page.goto(`/p/${two.id}/s/${session.id}`);
  await expect(
    page.getByRole("heading", { name: "内容不存在", exact: true }),
  ).toBeVisible();
  await expect(page.getByText("不能泄漏的会话")).not.toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`/p/${one.id}`);
  await expect(
    page.getByRole("heading", { name: "归属项目一", exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.getByRole("button", { name: "展开导航" }).click();
  await expect(
    page.getByRole("navigation", { name: "会话列表" }),
  ).toBeVisible();
});

test("网络请求失败时保留创建表单供重试", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "新建项目", exact: true }).click();
  const dialog = page.getByRole("dialog");
  await dialog.getByLabel("项目名称", { exact: true }).fill("断线后重试");
  await page.route("**/api/projects", (route) =>
    route.request().method() === "POST" ? route.abort() : route.continue(),
  );
  await dialog.getByRole("button", { name: "创建项目", exact: true }).click();
  await expect(dialog.getByRole("alert")).toContainText("无法连接");
  await expect(dialog.getByLabel("项目名称", { exact: true })).toHaveValue(
    "断线后重试",
  );
  await page.unroute("**/api/projects");
  await dialog.getByRole("button", { name: "创建项目", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "断线后重试", exact: true }),
  ).toBeVisible();
});

test("左下角模型设置可以切换并保存 Fixture", async ({ page, request }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "模型设置", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "模型设置", exact: true });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByLabel("模型模式")).toHaveValue("fixture");
  await dialog.getByLabel("模型模式").selectOption("remote");
  await expect(dialog.getByLabel("API Key")).toBeVisible();
  await dialog.getByLabel("API Key").fill("test-key");
  await dialog.getByLabel("模型端点").fill("http://127.0.0.1:19090/v1");
  await dialog.getByLabel("模型名称").fill("test-model");
  await dialog.getByRole("button", { name: "保存模型设置", exact: true }).click();
  await expect(dialog).not.toBeVisible();
  let response = await request.get("/api/runtime/model");
  expect(response.status()).toBe(200);
  expect(await response.json()).toMatchObject({ mode: "remote", model: "test-model", configured: true });

  await page.getByRole("button", { name: "模型设置", exact: true }).click();
  const fixtureDialog = page.getByRole("dialog", { name: "模型设置", exact: true });
  await fixtureDialog.getByLabel("模型模式").selectOption("fixture");
  await fixtureDialog.getByRole("button", { name: "保存模型设置", exact: true }).click();
  await expect(fixtureDialog).not.toBeVisible();
  response = await request.get("/api/runtime/model");
  expect(response.status()).toBe(200);
  expect((await response.json()).mode).toBe("fixture");
});

test("会话消息提供复制和重新发送操作", async ({ page, request }) => {
  const project = await createProject(request, "消息操作验收");
  const session = await (
    await request.post(`/api/projects/${project.id}/sessions`, {
      data: { title: "消息操作" },
    })
  ).json();
  await page.goto(`/p/${project.id}/s/${session.id}`);
  const text = "请回答项目里已有笔记说明了什么";
  await page.getByRole("textbox", { name: "消息", exact: true }).fill(text);
  await page.getByRole("button", { name: "发送消息" }).click();
  await expect(page.getByText(text, { exact: true })).toBeVisible();
  await expect(page.locator(".research-progress-card").first()).toBeVisible();
  await expect(page.getByText("completed", { exact: true })).toBeVisible();
  await expect(page.locator(".assistant-content").first()).toBeVisible();
  const userMessage = page.getByRole("article").filter({ hasText: text }).first();
  await expect(userMessage.getByRole("button", { name: "复制消息" })).toBeVisible();
  await page.context().grantPermissions(["clipboard-read", "clipboard-write"], { origin: new URL(page.url()).origin });
  await userMessage.getByRole("button", { name: "复制消息" }).click();
  await expect(userMessage.getByText("已复制", { exact: true })).toBeVisible();
  await expect(userMessage.getByRole("button", { name: "重新发送消息" })).toBeEnabled();
  await userMessage.getByRole("button", { name: "重新发送消息" }).click();
  await expect(page.getByText("completed", { exact: true })).toBeVisible();
  await expect(page.getByText(text, { exact: true })).toHaveCount(1);
});

test("开发模式可以查看 Graph 和模型运行详情", async ({ page, request }) => {
  const project = await createProject(request, "运行详情验收");
  const session = await (
    await request.post(`/api/projects/${project.id}/sessions`, {
      data: { title: "运行详情" },
    })
  ).json();
  await page.goto(`/p/${project.id}/s/${session.id}`);
  await page.getByRole("textbox", { name: "消息", exact: true }).fill("请回答项目里已有笔记说明了什么");
  await page.getByRole("button", { name: "发送消息" }).click();
  await expect(page.getByText("completed", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "开发运行详情", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "开发运行详情", exact: true });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByText("模型调用", { exact: true })).toBeVisible();
  await expect(dialog.getByLabel("运行指标").getByText("节点", { exact: true })).toBeVisible();
  await expect(dialog.getByRole("list", { name: "运行事件时间线" })).toContainText("模型");
  await expect(dialog).not.toContainText("请回答项目里已有笔记说明了什么");
});

test("重发不会追加重复消息且项目笔记可以打开", async ({ page, request }) => {
  const project = await createProject(request, "笔记引用验收");
  const session = await (
    await request.post(`/api/projects/${project.id}/sessions`, {
      data: { title: "笔记引用" },
    })
  ).json();
  await page.goto(`/p/${project.id}/s/${session.id}`);
  const text = "请帮我整理研究方向并保存为项目笔记";
  await page.getByRole("textbox", { name: "消息", exact: true }).fill(text);
  await page.getByRole("button", { name: "发送消息" }).click();
  await expect(page.getByRole("textbox", { name: "补充回答", exact: true })).toBeVisible();
  await expect(page.getByRole("radio", { name: "选择选项 1", exact: true })).toBeVisible();
  await page.getByRole("radio", { name: "选择选项 1", exact: true }).check();
  await expect(page.getByRole("textbox", { name: "补充回答", exact: true })).toHaveValue("先比较研究方向");
  await page.getByRole("textbox", { name: "编辑选项 1", exact: true }).fill("先比较研究方向（已编辑）");
  await expect(page.getByRole("textbox", { name: "补充回答", exact: true })).toHaveValue("先比较研究方向（已编辑）");
  await page.getByRole("button", { name: "提交回答" }).click();
  await expect(page.getByRole("button", { name: "展开项目笔记", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "展开项目笔记", exact: true }).click();
  await expect(page.getByRole("button", { name: "收起项目笔记", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "打开项目笔记", exact: true })).toBeVisible();

  const userMessage = page.getByRole("article").filter({ hasText: text }).first();
  await userMessage.getByRole("button", { name: "重新发送消息" }).click();
  await expect(page.getByText("completed", { exact: true })).toBeVisible();
  await expect(page.getByText(text, { exact: true })).toHaveCount(1);
  await page.getByRole("button", { name: "打开项目笔记", exact: true }).click();
  await expect(page.getByRole("dialog", { name: "项目笔记", exact: true })).toBeVisible();
});

test("点击确认即可删除会话并返回项目页", async ({ page, request }) => {
  const project = await createProject(request, "会话删除验收");
  const session = await (
    await request.post(`/api/projects/${project.id}/sessions`, {
      data: { title: "待删除会话" },
    })
  ).json();
  await page.goto(`/p/${project.id}/s/${session.id}`);
  await page.getByRole("navigation", { name: "会话列表" }).getByRole("button", { name: "删除会话", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "删除会话", exact: true });
  await expect(dialog.getByLabel("输入会话名称确认")).toHaveCount(0);
  await dialog.getByRole("button", { name: "确认永久删除" }).click();
  await expect(page).toHaveURL(`/p/${project.id}`);
  expect((await request.get(`/api/projects/${project.id}/sessions/${session.id}`)).status()).toBe(404);
});

test("项目笔记共享并可单条删除，删除不影响原会话", async ({ page, request }) => {
  const project = await createProject(request, "时间序列笔记验收");
  const session = await (await request.post(`/api/projects/${project.id}/sessions`, {
    data: { title: "整理方向" },
  })).json();
  await page.goto(`/p/${project.id}/s/${session.id}`);
  await page.getByRole("textbox", { name: "消息", exact: true }).fill("整理时间序列与大模型的研究方向并保存笔记");
  await page.getByRole("button", { name: "发送消息" }).click();
  await page.getByRole("textbox", { name: "补充回答", exact: true }).fill("先比较研究方向");
  await page.getByRole("button", { name: "提交回答" }).click();
  await expect(page.getByText("completed", { exact: true })).toBeVisible();
  const notes = page.getByRole("region", { name: "项目笔记", exact: true });
  await expect(notes).toContainText("同一项目的所有会话共享");
  await notes.getByRole("button", { name: "打开项目笔记", exact: true }).click();
  let dialog = page.getByRole("dialog", { name: "项目笔记", exact: true });
  await expect(dialog).toContainText("时间序列");
  await expect(dialog).not.toContainText("代码修复基线");
  await dialog.getByRole("button", { name: "关闭窗口" }).click();

  await page.getByRole("button", { name: "新建会话", exact: true }).click();
  await expect(notes).toContainText("1 条");
  await expect(notes).toContainText("来自其他会话");
  await notes.getByRole("button", { name: "打开项目笔记", exact: true }).click();
  dialog = page.getByRole("dialog", { name: "项目笔记", exact: true });
  await dialog.getByRole("button", { name: "删除笔记", exact: true }).click();
  let confirm = page.getByRole("dialog", { name: "删除项目笔记", exact: true });
  await expect(confirm).toContainText("后续新消息");
  await confirm.getByRole("button", { name: "取消", exact: true }).click();
  await expect(dialog.getByRole("button", { name: "删除笔记", exact: true })).toBeVisible();
  await dialog.getByRole("button", { name: "删除笔记", exact: true }).click();
  confirm = page.getByRole("dialog", { name: "删除项目笔记", exact: true });
  await confirm.getByRole("button", { name: "确认删除", exact: true }).click();
  await expect(dialog).toContainText("暂无项目笔记");
  await dialog.getByRole("button", { name: "关闭窗口" }).click();
  await page.reload();
  await expect(notes).toContainText("暂无项目笔记");
  const listed = await (await request.get(`/api/projects/${project.id}/notes`)).json();
  expect(listed.items).toEqual([]);
  const original = await (await request.get(`/api/projects/${project.id}/sessions/${session.id}/snapshot`)).json();
  expect(original.messages.length).toBeGreaterThan(1);
});

test("会话内容独立滚动，长消息不挤走标题和输入框", async ({ page, request }) => {
  const project = await createProject(request, "会话滚动验收");
  const session = await (await request.post(`/api/projects/${project.id}/sessions`, {
    data: { title: "长会话" },
  })).json();
  await page.goto(`/p/${project.id}/s/${session.id}`);
  const longText = "请解释以下时间序列基础概念：\n" + "观测值、时间戳、预测误差、季节性、缺失数据。\n".repeat(100);
  await page.getByRole("textbox", { name: "消息", exact: true }).fill(longText);
  await page.getByRole("button", { name: "发送消息" }).click();
  await expect(page.getByText("completed", { exact: true })).toBeVisible();
  for (const viewport of [{ width: 1440, height: 960 }, { width: 390, height: 844 }]) {
    await page.setViewportSize(viewport);
    const content = page.getByRole("region", { name: "会话内容", exact: true });
    await expect(content).toBeVisible();
    const result = await content.evaluate((element) => {
      element.scrollTop = 0;
      element.scrollTop = 200;
      return {
        scrollTop: element.scrollTop,
        clientHeight: element.clientHeight,
        scrollHeight: element.scrollHeight,
        pageScroll: document.documentElement.scrollHeight - window.innerHeight,
        horizontalScroll: document.documentElement.scrollWidth - window.innerWidth,
      };
    });
    expect(result.scrollTop).toBeGreaterThan(0);
    expect(result.scrollHeight).toBeGreaterThan(result.clientHeight);
    expect(result.pageScroll).toBeLessThanOrEqual(1);
    expect(result.horizontalScroll).toBeLessThanOrEqual(1);
    await expect(page.getByRole("heading", { name: "长会话", exact: true })).toBeInViewport();
    await expect(page.getByRole("textbox", { name: "消息", exact: true })).toBeInViewport();
  }
});
