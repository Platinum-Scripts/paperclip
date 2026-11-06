import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { runDatabaseBackup } from "./backup-lib.js";

vi.mock("postgres", () => {
  const sql = Object.assign(async () => [], {
    end: async () => {},
    unsafe: () => ({}),
  });
  return { default: () => sql };
});

vi.mock("node:zlib", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:zlib")>();
  return {
    ...actual,
    createGzip() {
      const stream = new PassThrough();
      const fail = (callback?: (error?: Error | null) => void) => {
        const error = new Error("compressor failed");
        stream.destroy(error);
        if (callback) process.nextTick(() => callback(error));
        return false;
      };
      stream.write = ((_chunk: unknown, encoding?: unknown, callback?: unknown) => {
        const done = typeof encoding === "function" ? encoding : callback;
        return fail(typeof done === "function" ? done as (error?: Error | null) => void : undefined);
      }) as typeof stream.write;
      return stream as unknown as ReturnType<typeof actual.createGzip>;
    },
  };
});

const directories: string[] = [];

function tempDir(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "backup-compressor-"));
  directories.push(directory);
  return directory;
}

afterEach(() => {
  for (const directory of directories.splice(0)) fs.rmSync(directory, { recursive: true, force: true });
});

describe("database backup compression errors", () => {
  it("rejects when compression fails, cancels the dump process, and keeps the failed partial", async () => {
    const backupDir = tempDir();
    const history = path.join(backupDir, "kept-history.sql.gz");
    const historyBytes = Buffer.from("kept-history");
    fs.writeFileSync(history, historyBytes);
    const marker = path.join(backupDir, "cancelled");
    const pidFile = path.join(backupDir, "dump.pid");
    const dump = path.join(backupDir, "dump-hang.js");
    fs.writeFileSync(dump, `#!/usr/bin/env node
import fs from "node:fs";
process.on("SIGTERM", () => {
  fs.writeFileSync(process.env.BACKUP_DUMP_MARKER, "sigterm");
  process.exit(0);
});
fs.writeFileSync(process.env.BACKUP_DUMP_PID, String(process.pid));
process.stdout.write("SELECT 1;\\n");
setInterval(() => {}, 200);
`, { mode: 0o700 });
    const previousDump = process.env.PAPERCLIP_PG_DUMP_PATH;
    const previousMarker = process.env.BACKUP_DUMP_MARKER;
    const previousPid = process.env.BACKUP_DUMP_PID;
    process.env.PAPERCLIP_PG_DUMP_PATH = dump;
    process.env.BACKUP_DUMP_MARKER = marker;
    process.env.BACKUP_DUMP_PID = pidFile;
    try {
      await expect(Promise.race([
        runDatabaseBackup({
          connectionString: "postgres://backup_user:S3cret-Backup-Pass@127.0.0.1:9/paperclip",
          backupDir,
          retention: { dailyDays: 7, weeklyWeeks: 4, monthlyMonths: 1 },
          filenamePrefix: "paperclip",
          backupEngine: "pg_dump",
        }),
        new Promise((_, reject) => setTimeout(() => reject(new Error("timed out waiting for dump cancellation")), 4000)),
      ])).rejects.toThrow(/compressor failed/);
      expect(fs.readFileSync(marker, "utf8")).toBe("sigterm");
      expect(fs.readFileSync(history)).toEqual(historyBytes);
      expect(fs.readdirSync(backupDir).filter((name) => name.endsWith(".sql.gz"))).toEqual(["kept-history.sql.gz"]);
      expect(fs.readdirSync(backupDir).some((name) => name.includes("partial"))).toBe(true);
      expect(fs.readdirSync(backupDir).some((name) => name.endsWith(".sql"))).toBe(false);
    } finally {
      if (fs.existsSync(pidFile)) {
        const pid = Number(fs.readFileSync(pidFile, "utf8"));
        try { process.kill(pid, "SIGKILL"); } catch { /* already exited */ }
      }
      if (previousDump === undefined) delete process.env.PAPERCLIP_PG_DUMP_PATH;
      else process.env.PAPERCLIP_PG_DUMP_PATH = previousDump;
      if (previousMarker === undefined) delete process.env.BACKUP_DUMP_MARKER;
      else process.env.BACKUP_DUMP_MARKER = previousMarker;
      if (previousPid === undefined) delete process.env.BACKUP_DUMP_PID;
      else process.env.BACKUP_DUMP_PID = previousPid;
    }
  });
});
