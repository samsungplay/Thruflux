import { cp, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import path from "node:path";

const root = process.cwd();
const sourceDir = path.join(root, "landing");
const targetDir = path.resolve(root, "..", "docs");
const packageJsonPath = path.join(root, "package.json");

await mkdir(targetDir, { recursive: true });
const entries = await readdir(sourceDir);
for (const entry of entries) {
  await cp(path.join(sourceDir, entry), path.join(targetDir, entry), {
    recursive: true,
    force: true,
  });
}

const packageJsonRaw = await readFile(packageJsonPath, "utf8");
const packageJson = JSON.parse(packageJsonRaw);
const appVersion = String(packageJson.version || "").trim();
if (appVersion.length > 0) {
  const docsIndexPath = path.join(targetDir, "index.html");
  const docsIndexHtml = await readFile(docsIndexPath, "utf8");
  const nextDocsIndexHtml = docsIndexHtml.replaceAll(
    "{{APP_VERSION}}",
    appVersion,
  );
  await writeFile(docsIndexPath, nextDocsIndexHtml, "utf8");
}
