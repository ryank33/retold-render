/**
 * Retold render worker (Render.com free tier = 512Mi).
 *
 * Memory strategy (works free + Standard):
 * - Pre-scale each photo to output size BEFORE concat (huge RAM win)
 * - One job at a time, single ffmpeg thread
 * - Cap slides / duration / lasers
 * - Defaults favor Standard (720p); override env on free tier if needed
 * - RENDER_LASERS=0 disables laser burn-in
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
app.use(express.json({ limit: "4mb" }));

const PORT = Number(process.env.PORT || 10000);
// Standard-plan defaults (override via env). Pre-scale still keeps RAM safe on free.
const OUT_W = Math.min(1920, Math.max(480, Number(process.env.RENDER_WIDTH || 1280)));
const OUT_H = Math.min(1080, Math.max(270, Number(process.env.RENDER_HEIGHT || 720)));
const OUT_FPS = Math.min(30, Math.max(10, Number(process.env.RENDER_FPS || 24)));
const MAX_SLIDES = Math.min(80, Math.max(1, Number(process.env.RENDER_MAX_SLIDES || 40)));
const MAX_TOTAL_SEC = Math.min(600, Math.max(20, Number(process.env.RENDER_MAX_SEC || 180)));
const MAX_LASERS = Math.min(80, Math.max(0, Number(process.env.RENDER_MAX_LASERS || 40)));
const ENABLE_LASERS =
  process.env.RENDER_LASERS === undefined ||
  process.env.RENDER_LASERS === "" ||
  process.env.RENDER_LASERS === "1" ||
  process.env.RENDER_LASERS === "true";
const FFMPEG_THREADS = Math.max(1, Number(process.env.FFMPEG_THREADS || 2));
const PRESET = process.env.FFMPEG_PRESET || "veryfast";
const CRF = process.env.FFMPEG_CRF || "23";

const jobs = new Map();
let chain = Promise.resolve();
let activeJobs = 0;

app.get("/health", (_req, res) => {
  res.json({
    ok: true,
    service: "retold-render",
    ffmpeg: true,
    audio: true,
    lasers: ENABLE_LASERS,
    out: `${OUT_W}x${OUT_H}@${OUT_FPS}`,
    maxSlides: MAX_SLIDES,
    maxSec: MAX_TOTAL_SEC,
    activeJobs,
    planHint: process.env.RENDER_PLAN_HINT || "standard",
    preset: PRESET,
    crf: CRF,
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

  if (activeJobs > 0) {
    // Still accept but queue — don't run parallel ffmpeg
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
  if (jobs.size > 30) {
    for (const [k, j] of jobs) {
      if (j.status === "ready" || j.status === "failed") {
        jobs.delete(k);
        if (jobs.size <= 20) break;
      }
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
    job.progress = 8;

    // Download + pre-scale one image at a time (never feed multi-MP source into encoder)
    const localFiles = [];
    for (let i = 0; i < timed.length; i += 1) {
      const s = timed[i];
      const rawPath = join(workDir, `raw-${String(i).padStart(3, "0")}.img`);
      const scaledPath = join(workDir, `slide-${String(i).padStart(3, "0")}.jpg`);
      await downloadToFile(s.url, rawPath);
      await scaleImageLowMem(rawPath, scaledPath);
      await fs.unlink(rawPath).catch(() => undefined);
      localFiles.push({ path: scaledPath, durationSec: s.durationSec });
      job.progress = 8 + Math.floor((i / timed.length) * 30);
      // Yield event loop between slides
      await new Promise((r) => setImmediate(r));
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

    // One audio track max on free tier
    const audioFiles = [];
    const tracks = (audioTracks || []).slice(0, 1);
    for (let i = 0; i < tracks.length; i += 1) {
      const t = tracks[i];
      if (!t?.url) continue;
      try {
        const dest = join(workDir, `audio-${i}.webm`);
        await downloadToFile(t.url, dest);
        // Re-encode audio to low-rate AAC intermediate to shrink decode RAM later
        const aacPath = join(workDir, `audio-${i}.m4a`);
        try {
          await runFfmpeg([
            "-y",
            "-threads",
            "1",
            "-i",
            dest,
            "-vn",
            "-c:a",
            "aac",
            "-b:a",
            "96k",
            "-ac",
            "1",
            "-ar",
            "44100",
            aacPath,
          ]);
          await fs.unlink(dest).catch(() => undefined);
          audioFiles.push({
            path: aacPath,
            offsetMs: Number(t.offsetMs) || 0,
            name: t.name || `track${i}`,
          });
        } catch {
          audioFiles.push({
            path: dest,
            offsetMs: Number(t.offsetMs) || 0,
            name: t.name || `track${i}`,
          });
        }
      } catch (err) {
        console.warn("[render] audio download skip", t.url, err?.message);
      }
    }
    job.progress = 45;

    const laserFilter =
      ENABLE_LASERS && events?.length ? buildLaserFilter(events, totalSec) : "";

    const silentPath = join(workDir, "silent.mp4");
    // Images already at OUT_WxOUT_H — keep vf tiny
    const vf = ["format=yuv420p", `fps=${OUT_FPS}`, laserFilter].filter(Boolean).join(",");

    await runFfmpeg([
      "-y",
      "-threads",
      String(FFMPEG_THREADS),
      "-filter_threads",
      String(FFMPEG_THREADS),
      "-filter_complex_threads",
      String(FFMPEG_THREADS),
      "-f",
      "concat",
      "-safe",
      "0",
      "-i",
      listPath,
      ...(vf ? ["-vf", vf] : []),
      "-c:v",
      "libx264",
      "-preset",
      PRESET,
      "-crf",
      CRF,
      "-tune",
      "stillimage",
      "-pix_fmt",
      "yuv420p",
      "-g",
      String(OUT_FPS * 2),
      "-movflags",
      "+faststart",
      "-an",
      "-t",
      String(Math.max(1, totalSec)),
      silentPath,
    ]);
    job.progress = 70;

    // Free scaled JPEGs before mix
    for (const f of localFiles) {
      await fs.unlink(f.path).catch(() => undefined);
    }

    const outPath = join(workDir, "quickshare.mp4");

    if (audioFiles.length === 0) {
      await fs.rename(silentPath, outPath).catch(async () => {
        await fs.copyFile(silentPath, outPath);
        await fs.unlink(silentPath).catch(() => undefined);
      });
    } else {
      const args = [
        "-y",
        "-threads",
        String(FFMPEG_THREADS),
        "-i",
        silentPath,
        "-i",
        audioFiles[0].path,
        "-filter_complex",
        `[1:a]aresample=44100,aformat=sample_fmts=fltp:channel_layouts=mono,adelay=${Math.max(0, audioFiles[0].offsetMs)}|${Math.max(0, audioFiles[0].offsetMs)},volume=1.0[aout]`,
        "-map",
        "0:v:0",
        "-map",
        "[aout]",
        "-c:v",
        "copy",
        "-c:a",
        "aac",
        "-b:a",
        "96k",
        "-ac",
        "1",
        "-shortest",
        "-movflags",
        "+faststart",
        outPath,
      ];
      try {
        await runFfmpeg(args);
        await fs.unlink(silentPath).catch(() => undefined);
      } catch (err) {
        console.warn("[render] audio mix failed, shipping silent", err?.message);
        await fs.rename(silentPath, outPath).catch(async () => {
          await fs.copyFile(silentPath, outPath);
        });
      }
      for (const a of audioFiles) await fs.unlink(a.path).catch(() => undefined);
    }

    job.progress = 90;
    const stat = await fs.stat(outPath);
    job.byteSize = stat.size;

    if (callbackUrl) {
      const fileBuf = await fs.readFile(outPath);
      // free file ASAP after read
      await fs.unlink(outPath).catch(() => undefined);
      const headers = {
        "Content-Type": "video/mp4",
        "Content-Length": String(fileBuf.byteLength),
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
        body: fileBuf,
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

/** Scale one image to output size with low-thread ffmpeg; writes JPEG. */
function scaleImageLowMem(inputPath, outputPath) {
  return runFfmpeg([
    "-y",
    "-threads",
    "1",
    "-i",
    inputPath,
    "-vf",
    `scale=${OUT_W}:${OUT_H}:force_original_aspect_ratio=decrease,pad=${OUT_W}:${OUT_H}:(ow-iw)/2:(oh-ih)/2,format=yuvj420p`,
    "-frames:v",
    "1",
    "-q:v",
    "5",
    outputPath,
  ]);
}

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
    const boxW = Math.round(OUT_W * 0.1);
    const boxH = Math.round(OUT_H * 0.1);
    const bx = Math.max(0, Math.min(OUT_W - boxW, Math.round(xN * OUT_W - boxW / 2)));
    const by = Math.max(0, Math.min(OUT_H - boxH, Math.round(yN * OUT_H - boxH / 2)));
    const t1 = Math.min(totalSec, t0 + 0.4);
    parts.push(
      `drawbox=x=${bx}:y=${by}:w=${boxW}:h=${boxH}:color=${color}@0.35:t=3:enable='between(t\\,${t0.toFixed(3)}\\,${t1.toFixed(3)})'`,
    );
    count += 1;
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
        durationSec = 3.5;
      }
    }
    durationSec = Math.min(20, Math.max(1.2, durationSec));
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
    headers: { "User-Agent": "retold-render/1.3" },
    redirect: "follow",
  });
  if (!res.ok) throw new Error(`download failed ${res.status} ${url}`);
  const body = res.body;
  if (!body) throw new Error("empty body");
  await pipeline(Readable.fromWeb(body), createWriteStream(dest));
}

function runFfmpeg(args) {
  return new Promise((resolve, reject) => {
    const proc = spawn("ffmpeg", ["-hide_banner", "-loglevel", "error", ...args], {
      stdio: ["ignore", "ignore", "pipe"],
      env: {
        ...process.env,
        OMP_NUM_THREADS: "1",
        OPENBLAS_NUM_THREADS: "1",
      },
    });
    let err = "";
    proc.stderr.on("data", (d) => {
      err += d.toString();
      if (err.length > 4000) err = err.slice(-3000);
    });
    proc.on("error", reject);
    proc.on("close", (code, signal) => {
      if (code === 0) resolve();
      else if (signal === "SIGKILL" || code === 137) {
        reject(
          new Error(
            "ffmpeg OOM-killed (512Mi free plan). Video was too heavy — try fewer photos or upgrade Render RAM.",
          ),
        );
      } else {
        reject(new Error(`ffmpeg exit ${code}: ${err.slice(-700)}`));
      }
    });
  });
}

app.listen(PORT, () => {
  console.log(
    `[retold-render] :${PORT} out=${OUT_W}x${OUT_H}@${OUT_FPS} lasers=${ENABLE_LASERS} maxSlides=${MAX_SLIDES}`,
  );
});
