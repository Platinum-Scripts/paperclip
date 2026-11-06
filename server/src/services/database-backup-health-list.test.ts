import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { inspectDatabaseBackupHealth } from "./database-backup-health.js";

const directories: string[] = [];

function tempDir(): string {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "backup-health-list-"));
  directories.push(directory);
  return directory;
}

afterEach(() => {
  for (const directory of directories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe("database backup health listing", () => {
  it("lists a completed sql.gz archive and ignores uncompressed sql, partials, and the lock file", () => {
    const backupDir = tempDir();
    const completed = path.join(backupDir, "paperclip-20260102-030405.sql.gz");
    fs.writeFileSync(completed, "completed-gzip");
    fs.writeFileSync(path.join(backupDir, "paperclip-20260102-030405.sql"), "uncompressed sql");
    fs.writeFileSync(path.join(backupDir, "paperclip-20260102-030405.sql.gz.partial-old"), "failed partial");
    fs.writeFileSync(path.join(backupDir, ".paperclip-backup.lock"), "{\"pid\":1}");

    const status = inspectDatabaseBackupHealth({
      enabled: true,
      backupDir,
      maxAgeHours: 26,
      now: new Date(),
    });

    expect(status.latestBackup?.name).toBe("paperclip-20260102-030405.sql.gz");
    expect(status.latestBackup?.path).toBe(completed);
    expect(status.warnings.map((warning) => warning.code)).not.toContain("database_backup_missing");
    expect(fs.existsSync(path.join(backupDir, "paperclip-20260102-030405.sql"))).toBe(true);
    expect(fs.existsSync(path.join(backupDir, "paperclip-20260102-030405.sql.gz.partial-old"))).toBe(true);
  });

  it("does not report a failed partial or uncompressed sql file as a completed backup", () => {
    const backupDir = tempDir();
    fs.writeFileSync(path.join(backupDir, "paperclip-20260102-030405.sql"), "uncompressed sql");
    fs.writeFileSync(path.join(backupDir, "paperclip-20260102-030405.sql.gz.partial-old"), "failed partial");

    const status = inspectDatabaseBackupHealth({
      enabled: true,
      backupDir,
      maxAgeHours: 26,
      now: new Date(),
    });

    expect(status.latestBackup).toBeNull();
    expect(status.warnings.map((warning) => warning.code)).toContain("database_backup_missing");
  });
});