import { runDatabaseBackup } from "./backup-lib.js";

const [backupDir, connectionString, dumpPath] = process.argv.slice(2);
if (!backupDir || !connectionString || !dumpPath) {
  process.stderr.write("usage: backup-overlap-child <backup-dir> <connection-string> <dump-path>\n");
  process.exit(2);
}

process.env.PAPERCLIP_PG_DUMP_PATH = dumpPath;

try {
  const result = await runDatabaseBackup({
    connectionString,
    backupDir,
    retention: { dailyDays: 7, weeklyWeeks: 4, monthlyMonths: 1 },
    filenamePrefix: "paperclip-proc",
    backupEngine: "pg_dump",
    connectTimeoutSeconds: 10,
  });
  process.stdout.write(`OK ${result.backupFile}\n`);
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  process.stdout.write(`ERR ${message}\n`);
  process.exitCode = 1;
}
