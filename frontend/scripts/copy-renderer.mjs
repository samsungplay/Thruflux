import { cp, mkdir } from "node:fs/promises";
import path from "node:path";

const root = process.cwd();
const src = path.join(root, "src", "renderer", "index.html");
const outDir = path.join(root, "dist", "renderer");
const dst = path.join(outDir, "index.html");

await mkdir(outDir, { recursive: true });
await cp(src, dst);
