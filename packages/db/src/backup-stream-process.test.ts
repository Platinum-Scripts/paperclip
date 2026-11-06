import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";
import { afterEach, describe, expect, it } from "vitest";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./test-embedded-postgres.js";

const cleanups: Array<() => Promise<void> | void> = [];
const children: ChildProcess[] = [];
const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

function tempDir(prefix: string): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  cleanups.push(() => fs.rmSync(directory, { recursive: true, force: true }));
  return directory;
}

function writeExecutable(filePath: string, contents: string): string {
  fs.writeFileSync(filePath, contents, { mode: 0o700 });
  return filePath;
}

function holdScript(ready: string, release: string, body: string): string {
  return `#!/usr/bin/env node
import fs from "node:fs";
fs.writeFileSync(${JSON.stringify(ready)}, "ready");
const timer = setInterval(() => {
  if (fs.existsSync(${JSON.stringify(release)})) {
    clearInterval(timer);
    process.stdout.write(${JSON.stringify(body)});
    process.exit(0);
  }
}, 20);
`;
}

async function createTempDatabase(): Promise<string> {
  const db = await startEmbeddedPostgresTestDatabase("paperclip-backup-proc-");
  cleanups.push(db.cleanup);
  return db.connectionString;
}

function spawnBackup(backupDir: string, connectionString: string, dumpPath: string): ChildProcess {
  const child = spawn(process.execPath, [
    "--import",
    "tsx",
    fileURLToPath(new URL("./backup-overlap-child.ts", import.meta.url)),
    backupDir,
    connectionString,
    dumpPath,
  ], {
    cwd: fileURLToPath(new URL(".", import.meta.url)),
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env,
  });
  children.push(child);
  return child;
}

async function readChild(child: ChildProcess, timeoutMs: number): Promise<{ code: number | null; output: string }> {
  let output = "";
  child.stdout?.setEncoding("utf8");
  child.stderr?.setEncoding("utf8");
  child.stdout?.on("data", (chunk) => { output += chunk; });
  child.stderr?.on("data", (chunk) => { output += chunk; });
  const code = await new Promise<number | null>((resolve, reject) => {
    const timer = setTimeout(() => resolve(null), timeoutMs);
    child.once("exit", (exitCode) => {
      clearTimeout(timer);
      resolve(exitCode);
    });
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
  });
  return { code, output };
}

async function waitForFile(filePath: string): Promise<void> {
  for (let attempt = 0; attempt < 100 && !fs.existsSync(filePath); attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.exitCode === null && !child.killed) child.kill("SIGKILL");
  }
  while (cleanups.length > 0) await cleanups.pop()?.();
});

if (!embeddedPostgresSupport.supported) {
  console.warn(`Skipping cross-process backup tests: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`);
}

describeEmbeddedPostgres("cross-process database backup exclusion", () => {
  it("blocks another process for the same backup directory and allows a different directory", async () => {
    const connectionString = await createTempDatabase();
    const admin = new URL(connectionString);
    const password = decodeURIComponent(admin.password);
    const backupDir = tempDir("backup-proc-same-");
    const otherDir = tempDir("backup-proc-other-");
    const release = path.join(os.tmpdir(), `backup-proc-release-${process.pid}-${Date.now()}`);
    cleanups.push(() => fs.rmSync(release, { force: true }));
    const ready = path.join(backupDir, "ready");
    const otherReady = path.join(otherDir, "ready");
    const dump = writeExecutable(path.join(backupDir, "dump.js"), holdScript(ready, release, "SELECT same;\n"));
    const otherDump = writeExecutable(path.join(otherDir, "dump.js"), holdScript(otherReady, release, "SELECT other;\n"));
    const history = path.join(backupDir, "kept-history.sql.gz");
    const historyBytes = Buffer.from("kept-history");
    fs.writeFileSync(history, historyBytes);

    const holder = spawnBackup(backupDir, connectionString, dump);
    await waitForFile(ready);
    expect(fs.existsSync(ready)).toBe(true);
    const overlapping = await readChild(spawnBackup(backupDir, connectionString, dump), 4000);
    expect(overlapping.output).toMatch(/already in progress/);
    expect(overlapping.output).not.toContain(password);

    const independent = spawnBackup(otherDir, connectionString, otherDump);
    await waitForFile(otherReady);
    expect(fs.existsSync(otherReady)).toBe(true);

    fs.writeFileSync(release, "go");
    const held = await readChild(holder, 20000);
    const other = await readChild(independent, 20000);
    expect(held.output).toMatch(/^OK /);
    expect(other.output).toMatch(/^OK /);
    const heldFile = held.output.trim().slice(3);
    const otherFile = other.output.trim().slice(3);
    expect(gunzipSync(fs.readFileSync(heldFile)).toString("utf8")).toBe("SELECT same;\n");
    expect(gunzipSync(fs.readFileSync(otherFile)).toString("utf8")).toBe("SELECT other;\n");
    expect(fs.readFileSync(history)).toEqual(historyBytes);
  }, 60_000);

  it("clears a same-instance lock after the holder process is killed", async () => {
    const connectionString = await createTempDatabase();
    const backupDir = tempDir("backup-proc-stale-");
    const release = path.join(backupDir, "release");
    const ready = path.join(backupDir, "ready");
    const dump = writeExecutable(path.join(backupDir, "dump.js"), holdScript(ready, release, "SELECT 1;\n"));
    const success = writeExecutable(path.join(backupDir, "dump-ok.sh"), "#!/bin/sh\nprintf 'SELECT recovered;\\n'\n");
    const holder = spawnBackup(backupDir, connectionString, dump);
    await waitForFile(ready);
    expect(fs.existsSync(ready)).toBe(true);
    const blocked = await readChild(spawnBackup(backupDir, connectionString, success), 4000);
    expect(blocked.output).toMatch(/already in progress/);
    holder.kill("SIGKILL");
    await readChild(holder, 5000);
    const recovered = await readChild(spawnBackup(backupDir, connectionString, success), 20000);
    expect(recovered.output).toMatch(/^OK /);
    expect(gunzipSync(fs.readFileSync(recovered.output.trim().slice(3))).toString("utf8")).toBe("SELECT recovered;\n");
  }, 60_000);
});
