import { spawn, execFileSync } from "node:child_process";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { createServer } from "node:net";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// 此入口仅为浏览器测试创建自己的服务和临时数据库。
const webDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const root = resolve(webDir, "..");
const scratch = await mkdtemp(resolve(tmpdir(), "autoresearch-e2e-"));
const binaryDir = resolve(root, ".cache", "e2e");
await mkdir(binaryDir, { recursive: true });
const binary = resolve(
  binaryDir,
  process.platform === "win32" ? "autoresearch.exe" : "autoresearch",
);
execFileSync("go", ["build", "-o", binary, "."], {
  cwd: root,
  stdio: "inherit",
});

async function unusedPort() {
  const server = createServer();
  await new Promise((done, fail) => {
    server.once("error", fail);
    server.listen(0, "127.0.0.1", done);
  });
  const port = server.address().port;
  await new Promise((done) => server.close(done));
  return port;
}

const backendPort = await unusedPort();
const config = resolve(scratch, "config.json");
await writeFile(
  config,
  JSON.stringify({
    listen: `127.0.0.1:${backendPort}`,
    data_dir: resolve(scratch, "data"),
    model_mode: "fixture",
  }),
);
const backend = spawn(binary, ["serve", "--config", config], {
  cwd: root,
  stdio: "inherit",
  windowsHide: true,
});

async function waitReady() {
  for (let attempt = 0; attempt < 100; attempt++) {
    if (backend.exitCode !== null) throw new Error("测试后端提前退出");
    try {
      if ((await fetch(`http://127.0.0.1:${backendPort}/readyz`)).ok) return;
    } catch {
      /* 等待自建服务就绪。 */
    }
    await new Promise((done) => setTimeout(done, 100));
  }
  throw new Error("测试后端未就绪");
}

let frontend;
function stop() {
  frontend?.kill();
  backend.kill();
}
process.on("SIGINT", () => {
  stop();
  process.exit(0);
});
process.on("SIGTERM", () => {
  stop();
  process.exit(0);
});
process.on("exit", stop);
await waitReady();
frontend = spawn(
  process.execPath,
  [
    "node_modules/vite/bin/vite.js",
    "--host",
    "127.0.0.1",
    "--port",
    process.env.AUTORESEARCH_E2E_PORT || "48781",
    "--strictPort",
  ],
  {
    cwd: webDir,
    env: {
      ...process.env,
      AUTORESEARCH_API_URL: `http://127.0.0.1:${backendPort}`,
    },
    stdio: "inherit",
    windowsHide: true,
  },
);
for (const child of [backend, frontend]) {
  child.on("error", (error) => {
    console.error(error);
    stop();
    process.exit(1);
  });
  child.on("exit", (code) => {
    stop();
    process.exit(code || 0);
  });
}
