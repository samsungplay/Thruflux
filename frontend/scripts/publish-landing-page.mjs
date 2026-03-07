import { cp, mkdir, readdir } from "node:fs/promises";
import path from "node:path";

const root = process.cwd();
const sourceDir = path.join(root, "landing");
const targetDir = path.resolve(root, "..", "docs");

await mkdir(targetDir, { recursive: true });
const entries = await readdir(sourceDir);
for (const entry of entries) {
  await cp(path.join(sourceDir, entry), path.join(targetDir, entry), {
    recursive: true,
    force: true,
  });
}
