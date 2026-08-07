/**
 * Retold render worker (Render.com).
 *
 * POST /render
 *   Body: {
 *     sessionId, showId, title,
 *     masterClockT0,
 *     slides: [{ index, url, caption, startMs, endMs }],
 *     callbackUrl,  // Worker endpoint to receive the MP4
 *     callbackSecret
 *   }
 *
 * Builds a silent 1080p H.264 slideshow from slide URLs + timings, then
 * POSTs the MP4 to the Worker (which stores it in R2 retold-out).
 */

import { createWriteStream, promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import express from "express";

const app = express();
app.use(express.json({ limit: "8mb" }));

const PORT = Number(process.env.PORT || 10000);
const jobs = new Map();

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "retold-render", ffmpeg: true });
});

app.get("/jobs/:id", (req, res) => {
  const job = jobs.get(req.params.id);
  if (!job) return res.status(404).json({ error: "not found" });
  res.json({
    id: job.id,
    status: job.status,
    progress: job.progress,
    error: job.error,
    videoUrl: job.videoUrl,
    byteSize: job.byteSize,
  });
});

app.post("/render", async (req, res) => {
  const body = req.body || {};
  const {
    sessionId,
    showId,
    title,
    slides,
    callbackUrl,
    callbackSecret,
  } = body;

  if (!sessionId || !Array.isArray(slides) || slides.length === 0) {
    return res.status(400).json({ error: "sessionId and slides[] required" });
  }

  const id = randomUUID();
  const job = {
    id,
    sessionId,
    showId,
    title: title || "Retold show",
    status: "queued",
    progress: 0,
    error: null,
    videoUrl: null,
    byteSize: null,
  };
  jobs.set(id, job);
  res.status(202).json({ jobId: id, status: "queued" });

  // Fire-and-forget
  runJob(job, slides, callbackUrl, callbackSecret).catch((err) => {
    job.status = "failed";
    job.error = err instanceof Error ? err.message : String(err);
    console.error("[render] job failed", id, job.error);
  });
});

async function runJob(job, slides, callbackUrl, callbackSecret) {
  job.status = "rendering";
  job.progress = 5;
  const workDir = join(tmpdir(), `retold-${job.id}`);
  await fs.mkdir(workDir, { recursive: true });

  try {
    // Normalize timings
    const timed = normalizeSlides(slides);
    job.progress = 10;

    // Download images
    const localFiles = [];
    for (let i = 0; i < timed.length; i += 1) {
      const s = timed[i];
      const ext = guessExt(s.url);
      const dest = join(workDir, `slide-${String(i).padStart(3, "0")}.${ext}`);
      await downloadToFile(s.url, dest);
      localFiles.push({ path: dest, durationSec: s.durationSec });
      job.progress = 10 + Math.floor((i / timed.length) * 40);
    }

    // Build ffmpeg concat list (image2 demuxer with durations)
    const listPath = join(workDir, "list.txt");
    const listBody = localFiles
      .map((f) => {
        // escape single quotes for ffmpeg concat
        const p = f.path.replace(/'/g, "'\\''");
        return `file '${p}'\nduration ${f.durationSec.toFixed(3)}`;
      })
      .join("\n")
      // ffmpeg concat needs the last file repeated without duration
      .concat(`\nfile '${localFiles[localFiles.length - 1].path.replace(/'/g, "'\\''")}'\n`);
    await fs.writeFile(listPath, listBody, "utf8");

    const outPath = join(workDir, "quickshare.mp4");
    job.progress = 55;
    await runFfmpeg([
      "-y",
      "-f",
      "concat",
      "-safe",
      "0",
      "-i",
      listPath,
      "-vf",
      "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2,format=yuv420p,fps=30",
      "-c:v",
      "libx264",
      "-preset",
      "veryfast",
      "-crf",
      "23",
      "-movflags",
      "+faststart",
      "-an",
      outPath,
    ]);

    job.progress = 85;
    const buf = await fs.readFile(outPath);
    job.byteSize = buf.byteLength;

    if (callbackUrl) {
      const headers = {
        "Content-Type": "video/mp4",
        "X-Session-Id": job.sessionId,
        "X-Show-Id": job.showId || "",
        "X-Job-Id": job.id,
        "X-Title": encodeURIComponent(job.title),
      };
      if (callbackSecret) headers.Authorization = `Bearer ${callbackSecret}`;
      const up = await fetch(callbackUrl, {
        method: "POST",
        headers,
        body: buf,
      });
      if (!up.ok) {
        const t = await up.text().catch(() => "");
        throw new Error(`callback failed ${up.status}: ${t.slice(0, 200)}`);
      }
      const json = await up.json().catch(() => ({}));
      job.videoUrl = json.url || json.quickshareUrl || null;
    }

    job.status = "ready";
    job.progress = 100;
    console.log("[render] ready", job.id, job.byteSize, "bytes");
  } finally {
    // Best-effort cleanup
    fs.rm(workDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

function normalizeSlides(slides) {
  // Accept {url, startMs, endMs} or {url, durationSec}
  const sorted = [...slides].sort(
    (a, b) => (a.index ?? 0) - (b.index ?? 0) || (a.startMs ?? 0) - (b.startMs ?? 0),
  );
  return sorted.map((s, i) => {
    let durationSec = Number(s.durationSec);
    if (!Number.isFinite(durationSec) || durationSec <= 0) {
      const start = Number(s.startMs ?? 0);
      const end = Number(s.endMs);
      if (Number.isFinite(end) && end > start) durationSec = (end - start) / 1000;
      else if (sorted[i + 1] && sorted[i + 1].startMs != null) {
        durationSec = (Number(sorted[i + 1].startMs) - start) / 1000;
      } else {
        durationSec = 4;
      }
    }
    // Clamp: min 1.5s, max 30s per slide for silent preview
    durationSec = Math.min(30, Math.max(1.5, durationSec));
    return { url: s.url, durationSec, caption: s.caption || "" };
  });
}

function guessExt(url) {
  try {
    const u = new URL(url);
    const m = u.pathname.match(/\.(jpe?g|png|webp|gif)$/i);
    if (m) return m[1].toLowerCase().replace("jpeg", "jpg");
  } catch {
    /* ignore */
  }
  return "jpg";
}

async function downloadToFile(url, dest) {
  const res = await fetch(url, {
    headers: { "User-Agent": "retold-render/1.0" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`download failed ${res.status} ${url}`);
  const body = res.body;
  if (!body) throw new Error("empty body");
  await pipeline(Readable.fromWeb(body), createWriteStream(dest));
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });
    let err = "";
    proc.stderr.on("data", (d) => {
      err += d.toString();
    });
    proc.on("error", reject);
    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg exit ${code}: ${err.slice(-800)}`));
    });
  });
}

app.listen(PORT, () => {
  console.log(`[retold-render] listening on :${PORT}`);
});
