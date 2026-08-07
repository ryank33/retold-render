/**
 * Retold render worker (Render.com).
 *
 * POST /render
 *   {
 *     sessionId, showId, title,
 *     slides: [{ index, url, startMs, endMs, durationSec }],
 *     audioTracks: [{ participantId, name, url, offsetMs }],
 *     events: [{ timestamp, type, payload }],  // for laser burn-in
 *     callbackUrl, callbackSecret
 *   }
 *
 * 1080p H.264 slideshow + mixed audio + laser overlays from event log.
 * POSTs MP4 to Worker → R2 retold-out.
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
app.use(express.json({ limit: "16mb" }));

const PORT = Number(process.env.PORT || 10000);
const jobs = new Map();

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "retold-render", ffmpeg: true, audio: true, lasers: true });
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
    audioTracks = [],
    events = [],
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

  runJob(job, { slides, audioTracks, events, callbackUrl, callbackSecret }).catch((err) => {
    job.status = "failed";
    job.error = err instanceof Error ? err.message : String(err);
    console.error("[render] job failed", id, job.error);
  });
});

async function runJob(job, { slides, audioTracks, events, callbackUrl, callbackSecret }) {
  job.status = "rendering";
  job.progress = 5;
  const workDir = join(tmpdir(), `retold-${job.id}`);
  await fs.mkdir(workDir, { recursive: true });

  try {
    const timed = normalizeSlides(slides);
    const totalSec = timed.reduce((a, s) => a + s.durationSec, 0);
    job.progress = 10;

    const localFiles = [];
    for (let i = 0; i < timed.length; i += 1) {
      const s = timed[i];
      const ext = guessExt(s.url);
      const dest = join(workDir, `slide-${String(i).padStart(3, "0")}.${ext}`);
      await downloadToFile(s.url, dest);
      localFiles.push({ path: dest, durationSec: s.durationSec });
      job.progress = 10 + Math.floor((i / timed.length) * 25);
    }

    const listPath = join(workDir, "list.txt");
    const listBody = localFiles
      .map((f) => {
        const p = f.path.replace(/'/g, "'\\''");
        return `file '${p}'\nduration ${f.durationSec.toFixed(3)}`;
      })
      .join("\n")
      .concat(`\nfile '${localFiles[localFiles.length - 1].path.replace(/'/g, "'\\''")}'\n`);
    await fs.writeFile(listPath, listBody, "utf8");

    // Download audio tracks
    const audioFiles = [];
    for (let i = 0; i < (audioTracks || []).length; i += 1) {
      const t = audioTracks[i];
      if (!t?.url) continue;
      try {
        const dest = join(workDir, `audio-${i}.webm`);
        await downloadToFile(t.url, dest);
        audioFiles.push({
          path: dest,
          offsetMs: Number(t.offsetMs) || 0,
          name: t.name || `track${i}`,
        });
      } catch (err) {
        console.warn("[render] audio download skip", t.url, err?.message);
      }
    }
    job.progress = 45;

    // Laser filter graph from events
    const laserFilter = buildLaserFilter(events || [], totalSec);

    const silentPath = join(workDir, "silent.mp4");
    const vf = [
      "scale=1920:1080:force_original_aspect_ratio=decrease",
      "pad=1920:1080:(ow-iw)/2:(oh-ih)/2",
      "format=yuv420p",
      "fps=30",
      laserFilter,
    ]
      .filter(Boolean)
      .join(",");

    await runFfmpeg([
      "-y",
      "-f",
      "concat",
      "-safe",
      "0",
      "-i",
      listPath,
      "-vf",
      vf,
      "-c:v",
      "libx264",
      "-preset",
      "veryfast",
      "-crf",
      "23",
      "-movflags",
      "+faststart",
      "-an",
      "-t",
      String(Math.max(1, totalSec)),
      silentPath,
    ]);
    job.progress = 70;

    const outPath = join(workDir, "quickshare.mp4");

    if (audioFiles.length === 0) {
      await fs.copyFile(silentPath, outPath);
    } else {
      // Mix all audio under video: [0:v] + delayed audio tracks
      const args = ["-y", "-i", silentPath];
      for (const a of audioFiles) {
        args.push("-i", a.path);
      }

      const n = audioFiles.length;
      const filters = [];
      const mixInputs = [];
      for (let i = 0; i < n; i += 1) {
        const delay = Math.max(0, audioFiles[i].offsetMs);
        // aresample + adelay (ms for left/right) + volume
        filters.push(
          `[${i + 1}:a]aresample=48000,aformat=sample_fmts=fltp:channel_layouts=stereo,adelay=${delay}|${delay},volume=1.0[a${i}]`,
        );
        mixInputs.push(`[a${i}]`);
      }
      filters.push(
        `${mixInputs.join("")}amix=inputs=${n}:duration=longest:dropout_transition=0:normalize=0[aout]`,
      );

      args.push(
        "-filter_complex",
        filters.join(";"),
        "-map",
        "0:v:0",
        "-map",
        "[aout]",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-shortest",
        "-movflags",
        "+faststart",
        outPath,
      );

      try {
        await runFfmpeg(args);
      } catch (err) {
        console.warn("[render] audio mix failed, shipping silent", err?.message);
        await fs.copyFile(silentPath, outPath);
      }
    }

    job.progress = 90;
    const buf = await fs.readFile(outPath);
    job.byteSize = buf.byteLength;

    // Also keep a copy of first audio track for separate download path via callback metadata
    if (callbackUrl) {
      const headers = {
        "Content-Type": "video/mp4",
        "X-Session-Id": job.sessionId,
        "X-Show-Id": job.showId || "",
        "X-Job-Id": job.id,
        "X-Title": encodeURIComponent(job.title),
        "X-Has-Audio": audioFiles.length ? "1" : "0",
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
    console.log("[render] ready", job.id, job.byteSize, "bytes audio=", audioFiles.length);
  } finally {
    fs.rm(workDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

/** Build ffmpeg drawbox chain for laser spots/strokes (approx). */
function buildLaserFilter(events, totalSec) {
  const parts = [];
  const pointers = events.filter((e) => e.type === "pointer_stroke");
  // Cap overlays so the filter graph stays small
  const max = 80;
  let count = 0;
  for (const ev of pointers) {
    if (count >= max) break;
    const p = ev.payload || {};
    const t0 = Math.max(0, Number(ev.timestamp || 0) / 1000);
    if (t0 > totalSec + 1) continue;
    const color = sanitizeColor(p.color || "#f5c542");
    const pts = Array.isArray(p.points) && p.points.length ? p.points : null;
    const xN = Number(p.x ?? pts?.[0]?.x ?? 0.5);
    const yN = Number(p.y ?? pts?.[0]?.y ?? 0.5);
    if (p.mode === "draw" && pts && pts.length > 1) {
      // Sample up to 6 points along the stroke
      const step = Math.max(1, Math.floor(pts.length / 6));
      for (let i = 0; i < pts.length && count < max; i += step) {
        const pt = pts[i];
        const bx = Math.max(0, Math.min(1880, Math.round(Number(pt.x) * 1920 - 12)));
        const by = Math.max(0, Math.min(1040, Math.round(Number(pt.y) * 1080 - 12)));
        const t1 = Math.min(totalSec, t0 + 0.55);
        parts.push(
          `drawbox=x=${bx}:y=${by}:w=24:h=24:color=${color}@0.75:t=fill:enable='between(t\\,${t0.toFixed(3)}\\,${t1.toFixed(3)})'`,
        );
        count += 1;
      }
    } else {
      // Spotlight circle approx as thick box
      const bx = Math.max(0, Math.min(1760, Math.round(xN * 1920 - 80)));
      const by = Math.max(0, Math.min(920, Math.round(yN * 1080 - 80)));
      const t1 = Math.min(totalSec, t0 + 0.45);
      parts.push(
        `drawbox=x=${bx}:y=${by}:w=160:h=160:color=${color}@0.35:t=4:enable='between(t\\,${t0.toFixed(3)}\\,${t1.toFixed(3)})'`,
      );
      count += 1;
    }
  }
  return parts.join(",");
}

function sanitizeColor(hex) {
  const m = String(hex).match(/^#?([0-9a-fA-F]{6})$/);
  if (m) return `0x${m[1]}`;
  return "0xf5c542";
}

function normalizeSlides(slides) {
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
    durationSec = Math.min(45, Math.max(1.2, durationSec));
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
    headers: { "User-Agent": "retold-render/1.1" },
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
      else reject(new Error(`ffmpeg exit ${code}: ${err.slice(-900)}`));
    });
  });
}

app.listen(PORT, () => {
  console.log(`[retold-render] listening on :${PORT}`);
});
