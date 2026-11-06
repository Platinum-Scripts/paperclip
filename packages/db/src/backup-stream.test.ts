import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { gunzipSync } from "node:zlib";
import { afterEach, describe, expect, it, vi } from "vitest";
import { runDatabaseBackup } from "./backup-lib.js";

vi.mock("postgres", () => {
  const sql = Object.assign(async () => [], {
    end: async () => {},
    unsafe: () => ({}),
  });
  return { default: () => sql };
});

const directories: string[] = [];
const SECRET = "S3cret-Backup-Pass";
const connectionString = `postgres://backup_user:${SECRET}@127.0.0.1:9/paperclip`;
const retention = { dailyDays: 7, weeklyWeeks: 4, monthlyMonths: 1 };

function tempDir(prefix: string): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  directories.push(directory);
  return directory;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function writeExecutable(filePath: string, contents: string): string {
  fs.writeFileSync(filePath, contents, { mode: 0o700 });
  return filePath;
}

function holdScript(options: { ready: string; release: string; body: string; exitCode?: number }): string {
  return `#!/usr/bin/env node
import fs from "node:fs";
fs.writeFileSync(${JSON.stringify(options.ready)}, "ready");
const timer = setInterval(() => {
  if (fs.existsSync(${JSON.stringify(options.release)})) {
    clearInterval(timer);
    process.stdout.write(${JSON.stringify(options.body)});
    process.exit(${options.exitCode ?? 0});
  }
}, 20);
`;
}

async function sawPlainSqlDuring(directory: string, run: () => Promise<void>): Promise<boolean> {
  let sawPlainSql = false;
  const poll = setInterval(() => {
    if (fs.readdirSync(directory).some((name) => name.endsWith(".sql"))) sawPlainSql = true;
  }, 0);
  try {
    await run();
  } finally {
    clearInterval(poll);
    await new Promise((resolve) => setImmediate(resolve));
    if (fs.readdirSync(directory).some((name) => name.endsWith(".sql"))) sawPlainSql = true;
  }
  return sawPlainSql;
}

afterEach(() => {
  vi.useRealTimers();
  for (const directory of directories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe("database backup streaming", () => {
  it("gzips a small logical sql backup without staging an uncompressed sql file", async () => {
    const backupDir = tempDir("backup-stream-js-");
    const history = path.join(backupDir, "kept-history.sql.gz");
    const historyBytes = Buffer.from("kept-history");
    fs.writeFileSync(history, historyBytes);

    let result: Awaited<ReturnType<typeof runDatabaseBackup>> | undefined;
    const sawPlainSql = await sawPlainSqlDuring(backupDir, async () => {
      result = await runDatabaseBackup({
        connectionString,
        backupDir,
        retention,
        filenamePrefix: "paperclip",
        backupEngine: "javascript",
      });
    });

    expect(sawPlainSql).toBe(false);
    expect(result?.backupFile.endsWith(".sql.gz")).toBe(true);
    const payload = gunzipSync(fs.readFileSync(result!.backupFile)).toString("utf8");
    expect(payload.startsWith("-- Paperclip database backup\n")).toBe(true);
    expect(payload).toContain("BEGIN;");
    expect(payload).toContain("COMMIT;");
    expect(payload).toContain("-- paperclip statement breakpoint 69f6f3f1-42fd-46a6-bf17-d1d85f8f3900");
    expect(payload).not.toContain("FROM stdin;");
    expect(fs.statSync(result!.backupFile).mode & 0o777).toBe(0o600);
    expect(fs.readFileSync(history)).toEqual(historyBytes);
    expect(fs.readdirSync(backupDir).some((name) => name.endsWith(".sql"))).toBe(false);
  });

  it("reports a redacted javascript fallback and does not stage uncompressed sql when pg_dump fails", async () => {
    const backupDir = tempDir("backup-stream-fallback-");
    const previous = process.env.PAPERCLIP_PG_DUMP_PATH;
    process.env.PAPERCLIP_PG_DUMP_PATH = "/bin/false";
    let result: Awaited<ReturnType<typeof runDatabaseBackup>> | undefined;
    try {
      const sawPlainSql = await sawPlainSqlDuring(backupDir, async () => {
        result = await runDatabaseBackup({
          connectionString,
          backupDir,
          retention,
          filenamePrefix: "paperclip",
          backupEngine: "auto",
        });
      });
      expect(sawPlainSql).toBe(false);
    } finally {
      if (previous === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
      else process.env.PAPERCLIP_PG_DUMP_PATH = previous;
    }

    expect(result?.backupEngineUsed).toBe("javascript");
    expect(result?.engineFallbackReason).toMatch(/failed with exit code 1/);
    expect(result?.engineFallbackReason).not.toContain(SECRET);
    expect(JSON.stringify(result)).not.toContain(SECRET);
    const payload = gunzipSync(fs.readFileSync(result!.backupFile)).toString("utf8");
    expect(payload).toContain("COMMIT;");
  });
});

describe("database backup dump failures", () => {
  it("propagates a dump process error without publishing over an existing archive or logging the password", async () => {
    const backupDir = tempDir("backup-stream-dump-error-");
    const history = path.join(backupDir, "kept-history.sql.gz");
    const historyBytes = Buffer.from("do-not-delete");
    fs.writeFileSync(history, historyBytes);
    const dump = writeExecutable(path.join(backupDir, "dump-fail.js"), `#!/usr/bin/env node
process.stderr.write(process.argv.join("\\n") + "\\n");
if (process.env.PGPASSWORD) process.stderr.write(process.env.PGPASSWORD + "\\n");
process.stdout.write("SELECT 1;\\n");
process.exit(3);
`);
    const previous = process.env.PAPERCLIP_PG_DUMP_PATH;
    process.env.PAPERCLIP_PG_DUMP_PATH = dump;
    let message = "";
    try {
      await runDatabaseBackup({
        connectionString,
        backupDir,
        retention,
        filenamePrefix: "paperclip",
        backupEngine: "pg_dump",
      });
    } catch (error) {
      message = error instanceof Error ? error.message : String(error);
    } finally {
      if (previous === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
      else process.env.PAPERCLIP_PG_DUMP_PATH = previous;
    }
    expect(message).toMatch(/exit code 3/);
    expect(message).not.toContain(SECRET);

    expect(fs.readFileSync(history)).toEqual(historyBytes);
    expect(fs.readdirSync(backupDir).filter((name) => name.endsWith(".sql.gz"))).toEqual(["kept-history.sql.gz"]);
    expect(fs.readdirSync(backupDir).some((name) => name.includes("partial"))).toBe(true);
    expect(fs.readdirSync(backupDir).some((name) => name.endsWith(".sql"))).toBe(false);
  });

  it("does not overwrite an existing archive when the backup name collides", async () => {
    const backupDir = tempDir("backup-stream-collision-");
    const history = path.join(backupDir, "kept-history.sql.gz");
    const historyBytes = Buffer.from("kept-history");
    fs.writeFileSync(history, historyBytes);
    const fixed = new Date("2026-01-02T03:04:05.000Z");
    const RealDate = Date;
    class FrozenDate extends RealDate {
      constructor(...args: unknown[]) {
        if (args.length === 0) super(fixed.getTime());
        else super(...(args as ConstructorParameters<typeof RealDate>));
      }

      static override now(): number {
        return fixed.getTime();
      }
    }
    globalThis.Date = FrozenDate as DateConstructor;
    const pad = (value: number) => String(value).padStart(2, "0");
    const stamp = `${fixed.getFullYear()}${pad(fixed.getMonth() + 1)}${pad(fixed.getDate())}-${pad(fixed.getHours())}${pad(fixed.getMinutes())}${pad(fixed.getSeconds())}`;
    const firstPath = path.join(backupDir, `paperclip-${stamp}.sql.gz`);
    const firstDump = writeExecutable(path.join(backupDir, "dump-one.sh"), "#!/bin/sh\nprintf 'SELECT 1;\\n'\n");
    const secondDump = writeExecutable(path.join(backupDir, "dump-two.sh"), "#!/bin/sh\nprintf 'SELECT 2;\\n'\n");
    const previous = process.env.PAPERCLIP_PG_DUMP_PATH;
    try {
      process.env.PAPERCLIP_PG_DUMP_PATH = firstDump;
      const first = await runDatabaseBackup({
        connectionString,
        backupDir,
        retention,
        filenamePrefix: "paperclip",
        backupEngine: "pg_dump",
      });
      const firstBytes = fs.readFileSync(first.backupFile);
      expect(gunzipSync(firstBytes).toString("utf8")).toBe("SELECT 1;\n");
      process.env.PAPERCLIP_PG_DUMP_PATH = secondDump;
      const second = await runDatabaseBackup({
        connectionString,
        backupDir,
        retention,
        filenamePrefix: "paperclip",
        backupEngine: "pg_dump",
      });
      expect(fs.readFileSync(firstPath)).toEqual(firstBytes);
      expect(second.backupFile).not.toBe(first.backupFile);
      expect(gunzipSync(fs.readFileSync(second.backupFile)).toString("utf8")).toBe("SELECT 2;\n");
      expect(fs.readFileSync(history)).toEqual(historyBytes);
    } finally {
      globalThis.Date = RealDate;
      if (previous === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
      else process.env.PAPERCLIP_PG_DUMP_PATH = previous;
    }
  });
});

describe("database backup overlap", () => {
  it("excludes a second backup of the same directory until dump and compression finish, then releases the lock after failure", async () => {
    const backupDir = tempDir("backup-stream-lock-");
    const history = path.join(backupDir, "kept-history.sql.gz");
    const historyBytes = Buffer.from("kept-history");
    fs.writeFileSync(history, historyBytes);
    const ready = path.join(backupDir, "ready");
    const release = path.join(backupDir, "release");
    const dump = writeExecutable(path.join(backupDir, "dump-hold.js"), holdScript({
      ready,
      release,
      body: "SELECT 1;\n",
      exitCode: 1,
    }));
    const success = writeExecutable(path.join(backupDir, "dump-ok.sh"), "#!/bin/sh\nprintf 'SELECT 1;\\n'\n");
    const previous = process.env.PAPERCLIP_PG_DUMP_PATH;
    process.env.PAPERCLIP_PG_DUMP_PATH = dump;
    const first = runDatabaseBackup({
      connectionString,
      backupDir,
      retention,
      filenamePrefix: "paperclip",
      backupEngine: "pg_dump",
    });
    try {
      for (let attempt = 0; attempt < 50 && !fs.existsSync(ready); attempt += 1) await delay(20);
      expect(fs.existsSync(ready)).toBe(true);
      const overlap = await Promise.race([
        runDatabaseBackup({
          connectionString,
          backupDir,
          retention,
          filenamePrefix: "paperclip",
          backupEngine: "pg_dump",
        }).then(() => "resolved", (error: unknown) => error instanceof Error ? error.message : String(error)),
        delay(2000).then(() => "TIMEOUT"),
      ]);
      expect(overlap).toMatch(/already in progress/);
      expect(overlap).not.toContain(SECRET);
      fs.writeFileSync(release, "go");
      await expect(first).rejects.toThrow(/exit code 1/);
      expect(fs.readFileSync(history)).toEqual(historyBytes);
      process.env.PAPERCLIP_PG_DUMP_PATH = success;
      const next = await runDatabaseBackup({
        connectionString,
        backupDir,
        retention,
        filenamePrefix: "paperclip",
        backupEngine: "pg_dump",
      });
      expect(next.backupFile.endsWith(".sql.gz")).toBe(true);
      expect(gunzipSync(fs.readFileSync(next.backupFile)).toString("utf8")).toBe("SELECT 1;\n");
    } finally {
      if (!fs.existsSync(release)) fs.writeFileSync(release, "go");
      await first.catch(() => {});
      if (previous === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
      else process.env.PAPERCLIP_PG_DUMP_PATH = previous;
    }
  });

  it("lets independent backup directories run at the same time", async () => {
    const left = tempDir("backup-stream-left-");
    const right = tempDir("backup-stream-right-");
    const release = path.join(os.tmpdir(), `backup-release-${process.pid}-${Date.now()}`);
    const leftReady = path.join(left, "ready");
    const rightReady = path.join(right, "ready");
    const leftDump = writeExecutable(path.join(left, "dump.js"), holdScript({ ready: leftReady, release, body: "SELECT left;\n" }));
    const rightDump = writeExecutable(path.join(right, "dump.js"), holdScript({ ready: rightReady, release, body: "SELECT right;\n" }));
    const previous = process.env.PAPERCLIP_PG_DUMP_PATH;
    const start = (backupDir: string) => runDatabaseBackup({
      connectionString,
      backupDir,
      retention,
      filenamePrefix: "paperclip",
      backupEngine: "pg_dump",
    });
    try {
      process.env.PAPERCLIP_PG_DUMP_PATH = leftDump;
      const leftBackup = start(left);
      for (let attempt = 0; attempt < 50 && !fs.existsSync(leftReady); attempt += 1) await delay(20);
      process.env.PAPERCLIP_PG_DUMP_PATH = rightDump;
      const rightBackup = start(right);
      for (let attempt = 0; attempt < 50 && !fs.existsSync(rightReady); attempt += 1) await delay(20);
      expect(fs.existsSync(leftReady)).toBe(true);
      expect(fs.existsSync(rightReady)).toBe(true);
      fs.writeFileSync(release, "go");
      const [leftResult, rightResult] = await Promise.all([leftBackup, rightBackup]);
      expect(gunzipSync(fs.readFileSync(leftResult.backupFile)).toString("utf8")).toBe("SELECT left;\n");
      expect(gunzipSync(fs.readFileSync(rightResult.backupFile)).toString("utf8")).toBe("SELECT right;\n");
    } finally {
      if (!fs.existsSync(release)) fs.writeFileSync(release, "go");
      fs.rmSync(release, { force: true });
      if (previous === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
      else process.env.PAPERCLIP_PG_DUMP_PATH = previous;
    }
  });
});
