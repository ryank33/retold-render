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
 * Slideshow + mixed audio + laser overlays → MP4 → Worker → R2.
 *
 * Memory notes (exit 137 = SIGKILL / OOM on free/starter instances):
 * - Default output is 720p @ 24fps (not 1080p30)
 * - Only one ffmpeg job at a time
 * - Cap duration / slide count / laser overlays
 * - Stream callback upload (don't buffer whole file in Node)
 */

import { createReadStream, createWriteStream, promises as fs } from "node:fs";
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
/** Output width (720 default — free Render plans often OOM at 1080p). */
const OUT_W = Math.min(1920, Math.max(640, Number(process.env.RENDER_WIDTH || 1280)));
const OUT_H = Math.min(1080, Math.max(360, Number(process.env.RENDER_HEIGHT || 720)));
const OUT_FPS = Math.min(30, Math.max(12, Number(process.env.RENDER_FPS || 24)));
const MAX_SLIDES = Math.min(80, Math.max(1, Number(process.env.RENDER_MAX_SLIDES || 40)));
const MAX_TOTAL_SEC = Math.min(600, Math.max(30, Number(process.env.RENDER_MAX_SEC || 180)));
const MAX_LASERS = Math.min(40, Math.max(0, Number(process.env.RENDER_MAX_LASERS || 24)));
const FFMPEG_THREADS = Math.max(1, Number(process.env.FFMPEG_THREADS || 1));

const jobs = new Map();
/** Serialize renders — parallel ffmpeg jobs blow RAM on small instances. */
let chain = Promise.resolve();
let activeJobs = 0;

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "retold-render",
    ffmpeg: true,
    audio: true,
    lasers: true,
    out: `${OUT_W}x${OUT_H}@${OUT_FPS}`,
    activeJobs,
    queueDepth: jobs.size,
  });
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
  // Bound in-memory job map
  if (jobs.size > 40) {
    const oldest = [...jobs.keys()].slice(0, jobs.size - 40);
    for (const k of oldest) {
      const j = jobs.get(k);
      if (j && (j.status === "ready" || j.status === "failed")) jobs.delete(k);
    }
  }

  res.status(202).json({ jobId: id, status: "queued" });

  chain = chain
    .then(() => runJob(job, { slides, audioTracks, events, callbackUrl, callbackSecret }))
    .catch((err) => {
      job.status = "failed";
      job.error = err instanceof Error ? err.message : String(err);
      console.error("[render] job failed", id, job.error);
    });
});

async function runJob(job, { slides, audioTracks, events, callbackUrl, callbackSecret }) {
  activeJobs += 1;
  job.status = "rendering";
  job.progress = 5;
  const workDir = join(tmpdir(), `retold-${job.id}`);
  await fs.mkdir(workDir, { recursive: true });

  try {
    let timed = normalizeSlides(slides).slice(0, MAX_SLIDES);
    // Soft-cap total runtime so free tiers don't OOM on long sessions
    let totalSec = timed.reduce((a, s) => a + s.durationSec, 0);
    if (totalSec > MAX_TOTAL_SEC) {
      const scale = MAX_TOTAL_SEC / totalSec;
      timed = timed.map((s) => ({
        ...s,
        durationSec: Math.max(1.2, s.durationSec * scale),
      }));
      totalSec = timed.reduce((a, s) => a + s.durationSec, 0);
      console.warn("[render] capped duration to", totalSec.toFixed(1), "s");
    }
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

    if (!localFiles.length) throw new Error("no slides downloaded");

    const listPath = join(workDir, "list.txt");
    const listBody = localFiles
      .map((f) => {
        const p = f.path.replace(/'/g, "'\\''");
        return `file '${p}'\nduration ${f.durationSec.toFixed(3)}`;
      })
      .join("\n")
      .concat(`\nfile '${localFiles[localFiles.length - 1].path.replace(/'/g, "'\\''")}'\n`);
    await fs.writeFile(listPath, listBody, "utf8");

    // Download audio tracks (cap 2 tracks for RAM)
    const audioFiles = [];
    const tracks = (audioTracks || []).slice(0, 2);
    for (let i = 0; i < tracks.length; i += 1) {
      const t = tracks[i];
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

    const laserFilter = buildLaserFilter(events || [], totalSec);

    const silentPath = join(workDir, "silent.mp4");
    const vf = [
      `scale=${OUT_W}:${OUT_H}:force_original_aspect_ratio=decrease`,
      `pad=${OUT_W}:${OUT_H}:(ow-iw)/2:(oh-ih)/2`,
      "format=yuv420p",
      `fps=${OUT_FPS}`,
      laserFilter,
    ]
      .filter(Boolean)
      .join(",");

    await runFfmpeg([
      "-y",
      "-threads",
      String(FFMPEG_THREADS),
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
      // ultrafast + higher CRF = much less RAM/CPU on free instances
      "-preset",
      process.env.FFMPEG_PRESET || "ultrafast",
      "-crf",
      process.env.FFMPEG_CRF || "28",
      "-pix_fmt",
      "yuv420p",
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
      const args = ["-y", "-threads", String(FFMPEG_THREADS), "-i", silentPath];
      for (const a of audioFiles) {
        args.push("-i", a.path);
      }

      const n = audioFiles.length;
      const filters = [];
      const mixInputs = [];
      for (let i = 0; i < n; i += 1) {
        const delay = Math.max(0, audioFiles[i].offsetMs);
        filters.push(
          `[${i + 1}:a]aresample=44100,aformat=sample_fmts=fltp:channel_layouts=stereo,adelay=${delay}|${delay},volume=1.0[a${i}]`,
        );
        mixInputs.push(`[a${i}]`);
      }
      filters.push(
        `${mixInputs.join("")}amix=inputs=${n}:duration=first:dropout_transition=0:normalize=0[aout]`,
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
        "128k",
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
    const stat = await fs.stat(outPath);
    job.byteSize = stat.size;

    if (callbackUrl) {
      // Stream file to Worker — avoid loading entire MP4 into Node heap
      const headers = {
        "Content-Type": "video/mp4",
        "Content-Length": String(stat.size),
        "X-Session-Id": job.sessionId,
        "X-Show-Id": job.showId || "",
        "X-Job-Id": job.id,
        "X-Title": encodeURIComponent(job.title),
        "X-Has-Audio": audioFiles.length ? "1" : "0",
      };
      if (callbackSecret) headers.Authorization = `Bearer ${callbackSecret}`;

      const fileStream = createReadStream(outPath);
      const up = await fetch(callbackUrl, {
        method: "POST",
        headers,
        // Node 20+ supports duplex for streaming bodies
        body: fileStream,
        duplex: "half",
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
    console.log(
      "[render] ready",
      job.id,
      job.byteSize,
      "bytes",
      `${OUT_W}x${OUT_H}`,
      "audio=",
      audioFiles.length,
    );
  } finally {
    activeJobs = Math.max(0, activeJobs - 1);
    fs.rm(workDir, { recursive: true, force: true }).catch(() => undefined);
  }
}

/** Build ffmpeg drawbox chain for laser spots/strokes (approx). */
function buildLaserFilter(events, totalSec) {
  const parts = [];
  const pointers = events.filter((e) => e.type === "pointer_stroke");
  let count = 0;
  for (const ev of pointers) {
    if (count >= MAX_LASERS) break;
    const p = ev.payload || {};
    const t0 = Math.max(0, Number(ev.timestamp || 0) / 1000);
    if (t0 > totalSec + 1) continue;
    const color = sanitizeColor(p.color || "#f5c542");
    const pts = Array.isArray(p.points) && p.points.length ? p.points : null;
    const xN = Number(p.x ?? pts?.[0]?.x ?? 0.5);
    const yN = Number(p.y ?? pts?.[0]?.y ?? 0.5);
    if (p.mode === "draw" && pts && pts.length > 1) {
      const step = Math.max(1, Math.floor(pts.length / 4));
      for (let i = 0; i < pts.length && count < MAX_LASERS; i += step) {
        const pt = pts[i];
        const bx = Math.max(0, Math.min(OUT_W - 24, Math.round(Number(pt.x) * OUT_W - 12)));
        const by = Math.max(0, Math.min(OUT_H - 24, Math.round(Number(pt.y) * OUT_H - 12)));
        const t1 = Math.min(totalSec, t0 + 0.55);
        parts.push(
          `drawbox=x=${bx}:y=${by}:w=24:h=24:color=${color}@0.75:t=fill:enable='between(t\\,${t0.toFixed(3)}\\,${t1.toFixed(3)})'`,
        );
        count += 1;
      }
    } else {
      const boxW = Math.round(OUT_W * 0.08);
      const boxH = Math.round(OUT_H * 0.08);
      const bx = Math.max(0, Math.min(OUT_W - boxW, Math.round(xN * OUT_W - boxW / 2)));
      const by = Math.max(0, Math.min(OUT_H - boxH, Math.round(yN * OUT_H - boxH / 2)));
      const t1 = Math.min(totalSec, t0 + 0.45);
      parts.push(
        `drawbox=x=${bx}:y=${by}:w=${boxW}:h=${boxH}:color=${color}@0.35:t=4:enable='between(t\\,${t0.toFixed(3)}\\,${t1.toFixed(3)})'`,
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
    durationSec = Math.min(30, Math.max(1.2, durationSec));
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
    headers: { "User-Agent": "retold-render/1.2" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`download failed ${res.status} ${url}`);
  const body = res.body;
  if (!body) throw new Error("empty body");
  await pipeline(Readable.fromWeb(body), createWriteStream(dest));
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffmpeg", args, {
      stdio: ["ignore", "pipe", "pipe"],
      env: {
        ...process.env,
        // Keep ffmpeg from spawning huge thread pools
        OMP_NUM_THREADS: String(FFMPEG_THREADS),
      },
    });
    let err = "";
    proc.stderr.on("data", (d) => {
      err += d.toString();
      if (err.length > 8000) err = err.slice(-6000);
    });
    proc.on("error", reject);
    proc.on("close", (code, signal) => {
      if (code === 0) resolve();
      else if (signal === "SIGKILL" || code === 137) {
        reject(
          new Error(
            "ffmpeg killed (exit 137) — usually out of memory on free Render plans. Retry or upgrade RAM.",
          ),
        );
      } else {
        reject(new Error(`ffmpeg exit ${code}: ${err.slice(-900)}`));
      }
    });
  });
}

app.listen(PORT, () => {
  console.log(
    `[retold-render] listening on :${PORT} out=${OUT_W}x${OUT_H}@${OUT_FPS} threads=${FFMPEG_THREADS}`,
  );
});
