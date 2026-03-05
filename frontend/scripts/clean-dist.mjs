import { rm } from "node:fs/promises";
import path from "node:path";

const distDir = path.join(process.cwd(), "dist");
try {
  await rm(distDir, { recursive: true, force: true });
} catch (error) {
  const code =
    error && typeof error === "object" && "code" in error
      ? String(error.code)
      : "UNKNOWN";
  if (code === "EACCES" || code === "EPERM") {
    console.warn(`[clean] warning: could not fully clean dist (${code}).`);
    process.exit(0);
  }
  throw error;
}
