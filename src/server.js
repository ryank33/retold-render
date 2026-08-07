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
const RENDER_HOOK_KEY = process.env.RENDER_HOOK_KEY || "";

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

function bearer(req) {
  return String(req.headers.authorization || "").replace(/^Bearer\s+/i, "");
}

function adaptManifestJob(body) {
  const manifest = body.manifest || {};
  const rawEvents = Array.isArray(manifest.events) ? manifest.events : [];
  const durationMs = Math.max(1000, Number(manifest.duration_ms) || 1000);
  const slides = timedSlidesFromManifest(
    Array.isArray(manifest.slides) ? manifest.slides : [],
    rawEvents,
    durationMs,
  );
  const audioTracks = (Array.isArray(manifest.audio_tracks) ? manifest.audio_tracks : []).map((track) => ({
    name: track.name || track.email || track.user_id || "speaker",
    offsetMs: Number(track.start_offset) || 0,
    chunks: (Array.isArray(track.chunks) ? track.chunks : []).map((chunk) => ({
      url: chunk.url,
      offsetMs: Number(chunk.offset_ms) || 0,
      sequence: Number(chunk.sequence) || 0,
      contentType: chunk.content_type || track.mime || "audio/webm",
    })),
  }));

  return {
    jobId: body.job_id,
    sessionId: body.session_id || manifest.session_id,
    showId: manifest.show?.id,
    title: manifest.show?.title || "Retold show",
    slides,
    audioTracks,
    events: normalizeManifestEvents(rawEvents),
    callbackUrl: manifest.callback_url,
    statusCallbackUrl: manifest.status_callback_url,
    callbackSecret: RENDER_HOOK_KEY,
  };
}

function timedSlidesFromManifest(photos, events, durationMs) {
  if (!photos.length) return [];
  const byPosition = new Map(photos.map((photo, index) => [Number(photo.position ?? index), photo]));
  const changes = events
    .filter((event) => event.type === "slide.change")
    .map((event) => ({ t: Math.max(0, Number(event.t_ms) || 0), index: Number(event.payload?.index) || 0 }))
    .sort((a, b) => a.t - b.t);

  const segments = [];
  let current = 0;
  let start = 0;
  for (const change of changes) {
    const at = Math.min(durationMs, change.t);
    if (at > start) {
      const photo = byPosition.get(current) || photos[current] || photos[0];
      segments.push({ ...photo, index: segments.length, startMs: start, endMs: at, durationSec: (at - start) / 1000 });
    }
    current = change.index;
    start = at;
  }
  if (start < durationMs || !segments.length) {
    const photo = byPosition.get(current) || photos[current] || photos[0];
    segments.push({
      ...photo,
      index: segments.length,
      startMs: start,
      endMs: durationMs,
      durationSec: Math.max(0.1, (durationMs - start) / 1000),
    });
  }
  return segments;
}

function normalizeManifestEvents(events) {
  return events.map((event) => {
    if (event.type !== "pointer") return event;
    return {
      ...event,
      type: "pointer_stroke",
      timestamp: Number(event.t_ms) || 0,
      payload: {
        ...(event.payload || {}),
        mode: event.payload?.mode === "stroke" ? "draw" : "spotlight",
      },
    };
  });
}

app.post("/render", async (req, res) => {
  const rawBody = req.body || {};
  const isManifestJob = !!rawBody.manifest;
  if (isManifestJob && (!RENDER_HOOK_KEY || bearer(req) !== RENDER_HOOK_KEY)) {
    return res.status(403).json({ error: "bad render hook key" });
  }

  const body = isManifestJob ? adaptManifestJob(rawBody) : rawBody;
  const {
    sessionId,
    showId,
    title,
    slides,
    audioTracks = [],
    events = [],
    callbackUrl,
    callbackSecret,
    statusCallbackUrl,
  } = body;

  if (!sessionId || !Array.isArray(slides) || slides.length === 0) {
    return res.status(400).json({ error: "sessionId and slides[] required" });
  }

  if (activeJobs > 0) {
    // Still accept but queue — don't run parallel ffmpeg
  }

  const id = body.jobId || randomUUID();
  const existing = jobs.get(id);
  if (existing && (existing.status === "queued" || existing.status === "rendering" || existing.status === "ready")) {
    return res.status(202).json({ jobId: id, status: existing.status, duplicate: true });
  }
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
    .catch(async (err) => {
      job.status = "failed";
      job.error = err instanceof Error ? err.message : String(err);
      console.error("[render] job failed", id, job.error);
      await notifyFailure(statusCallbackUrl, callbackSecret, job.error).catch((callbackErr) => {
        console.error("[render] failure callback failed", callbackErr?.message);
      });
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

    // Build up to six independently timed participant tracks. Chunked captures
    // are mixed onto the master clock before the final video mix.
    const audioFiles = [];
    const tracks = (audioTracks || []).slice(0, 6);
    for (let i = 0; i < tracks.length; i += 1) {
      const t = tracks[i];
      try {
        const prepared = await prepareAudioTrack(workDir, t, i);
        if (prepared) audioFiles.push(prepared);
      } catch (err) {
        console.warn("[render] audio track skip", t?.name || i, err?.message);
      }
    }
    job.progress = 45;

    const silentPath = join(workDir, "silent.mp4");
    // Images already at OUT_WxOUT_H. Lasers burned via ASS vector paths (not blocky drawbox).
    const vfParts = ["format=yuv420p", `fps=${OUT_FPS}`];
    if (ENABLE_LASERS && events?.length) {
      const assPath = join(workDir, "lasers.ass");
      await fs.writeFile(assPath, buildAssLasers(events, totalSec, OUT_W, OUT_H), "utf8");
      // Escape path for ffmpeg filtergraph
      const assEsc = assPath.replace(/\\/g, "/").replace(/:/g, "\\:").replace(/,/g, "\\,");
      vfParts.push(`ass=${assEsc}`);
    }
    const vf = vfParts.join(",");

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
      "-vf",
      vf,
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
      ];
      for (const audio of audioFiles) args.push("-i", audio.path);
      const delayed = audioFiles.map((audio, index) => {
        const delay = Math.max(0, Number(audio.offsetMs) || 0);
        return `[${index + 1}:a]aresample=44100,aformat=sample_fmts=fltp:channel_layouts=mono,adelay=${delay}|${delay},volume=1.0[a${index}]`;
      });
      const mix = audioFiles.length === 1
        ? `${delayed[0]};[a0]apad[aout]`
        : `${delayed.join(";")};${audioFiles.map((_, i) => `[a${i}]`).join("")}amix=inputs=${audioFiles.length}:normalize=0:duration=longest:dropout_transition=0[mixed];[mixed]apad[aout]`;
      args.push(
        "-filter_complex",
        mix,
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
        "-t",
        String(Math.max(1, totalSec)),
        "-movflags",
        "+faststart",
        outPath,
      );
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

async function prepareAudioTrack(workDir, track, index) {
  const chunks = (Array.isArray(track?.chunks) ? track.chunks : [])
    .filter((chunk) => chunk?.url)
    .sort((a, b) => (Number(a.sequence) || 0) - (Number(b.sequence) || 0));
  const outputPath = join(workDir, `audio-${index}.m4a`);

  if (chunks.length) {
    const localChunks = [];
    for (let i = 0; i < chunks.length; i += 1) {
      const path = join(workDir, `audio-${index}-chunk-${String(i).padStart(3, "0")}.bin`);
      await downloadToFile(chunks[i].url, path);
      localChunks.push({ path, offsetMs: Math.max(0, Number(chunks[i].offsetMs) || 0) });
    }

    try {
      const args = ["-y", "-threads", "1"];
      for (const chunk of localChunks) args.push("-i", chunk.path);
      const delayed = localChunks.map((chunk, i) =>
        `[${i}:a]aresample=44100,aformat=sample_fmts=fltp:channel_layouts=mono,adelay=${chunk.offsetMs}|${chunk.offsetMs}[c${i}]`,
      );
      const mix = localChunks.length === 1
        ? `${delayed[0]};[c0]anull[trackout]`
        : `${delayed.join(";")};${localChunks.map((_, i) => `[c${i}]`).join("")}amix=inputs=${localChunks.length}:normalize=0:duration=longest:dropout_transition=0[trackout]`;
      args.push(
        "-filter_complex", mix,
        "-map", "[trackout]",
        "-vn",
        "-c:a", "aac",
        "-b:a", "96k",
        "-ac", "1",
        "-ar", "44100",
        outputPath,
      );
      await runFfmpeg(args);
    } finally {
      for (const chunk of localChunks) await fs.unlink(chunk.path).catch(() => undefined);
    }
    return { path: outputPath, offsetMs: 0, name: track.name || `track${index}` };
  }

  if (!track?.url) return null;
  const rawPath = join(workDir, `audio-${index}.raw`);
  await downloadToFile(track.url, rawPath);
  try {
    await runFfmpeg([
      "-y", "-threads", "1", "-i", rawPath, "-vn",
      "-c:a", "aac", "-b:a", "96k", "-ac", "1", "-ar", "44100", outputPath,
    ]);
    await fs.unlink(rawPath).catch(() => undefined);
    return { path: outputPath, offsetMs: Number(track.offsetMs) || 0, name: track.name || `track${index}` };
  } catch {
    return { path: rawPath, offsetMs: Number(track.offsetMs) || 0, name: track.name || `track${index}` };
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

/**
 * Burn lasers via ASS (libass).
 *
 * Critical: ASS vector drawings FILL by default. For draw trails we use
 * fully transparent primary (\1a&HFF&) + thick outline (\bord + \3c) so the
 * path is a stroke only — not a filled polygon blob.
 * Coordinates in event payload are normalized 0–1.
 */
function buildAssLasers(events, totalSec, width, height) {
  const pointers = events
    .filter((e) => e.type === "pointer_stroke")
    .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0));

  const lines = [];
  let lastSpotMs = -9999;
  let drawBudget = MAX_LASERS * 4;
  let spotBudget = MAX_LASERS * 2;
  const minDim = Math.min(width, height);
  // Spotlight ~22% of frame (readable ring), draw stroke ~1.4% thickness
  const spotFs = Math.round(minDim * 0.22);
  const spotCore = Math.round(minDim * 0.045);
  const strokeBord = Math.max(5, Math.round(minDim * 0.014));

  for (const ev of pointers) {
    const p = ev.payload || {};
    const t0 = Math.max(0, Number(ev.timestamp || 0) / 1000);
    if (t0 > totalSec + 0.5) continue;
    const mode = p.mode === "draw" ? "draw" : "spotlight";
    const color = hexToAss(p.color || "#f5c542");
    const pts = Array.isArray(p.points) ? p.points : [];

    if (mode === "spotlight") {
      if (spotBudget <= 0) continue;
      const ms = Math.round(t0 * 1000);
      if (ms - lastSpotMs < 80) continue;
      lastSpotMs = ms;
      spotBudget -= 1;
      const xN = Number(p.x ?? pts[0]?.x ?? 0.5);
      const yN = Number(p.y ?? pts[0]?.y ?? 0.5);
      if (!Number.isFinite(xN) || !Number.isFinite(yN)) continue;
      const px = Math.round(clamp01(xN) * width);
      const py = Math.round(clamp01(yN) * height);
      const t1 = Math.min(totalSec + 0.2, t0 + 0.55);
      // Large outer ring (outline-heavy) + brighter core
      lines.push(
        `Dialogue: 0,${formatAssTime(t0)},${formatAssTime(t1)},Laser,,0,0,0,,{\\an5\\pos(${px},${py})\\fs${spotFs}\\bord${Math.max(4, Math.round(spotFs * 0.07))}\\shad0\\1c${color}\\3c${color}\\1a&HC0&\\3a&H20&}○`,
      );
      lines.push(
        `Dialogue: 0,${formatAssTime(t0)},${formatAssTime(t1)},Laser,,0,0,0,,{\\an5\\pos(${px},${py})\\fs${spotCore}\\bord0\\shad0\\1c${color}\\1a&H40&}●`,
      );
      continue;
    }

    // Draw trail — stroke only (never fill the polygon)
    if (drawBudget <= 0) continue;
    const rawPts =
      pts.length >= 2
        ? pts
        : Number.isFinite(Number(p.x)) && Number.isFinite(Number(p.y))
          ? [{ x: Number(p.x), y: Number(p.y) }]
          : [];
    if (!rawPts.length) continue;

    const step = Math.max(1, Math.floor(rawPts.length / 64));
    const sampled = [];
    for (let i = 0; i < rawPts.length; i += step) sampled.push(rawPts[i]);
    const last = rawPts[rawPts.length - 1];
    if (sampled[sampled.length - 1] !== last) sampled.push(last);

    const scaled = sampled
      .map((pt) => {
        const x = Math.round(clamp01(Number(pt.x)) * width);
        const y = Math.round(clamp01(Number(pt.y)) * height);
        return Number.isFinite(x) && Number.isFinite(y) ? { x, y } : null;
      })
      .filter(Boolean);
    if (!scaled.length) continue;

    drawBudget -= 1;
    const t1 = Math.min(totalSec + 0.3, t0 + 1.25);

    if (scaled.length === 1) {
      const { x, y } = scaled[0];
      lines.push(
        `Dialogue: 0,${formatAssTime(t0)},${formatAssTime(t1)},Laser,,0,0,0,,{\\an5\\pos(${x},${y})\\fs${strokeBord * 2}\\bord0\\1c${color}\\1a&H30&}●`,
      );
      continue;
    }

    // Open path as STROKE: transparent fill (\1a&HFF&), thick outline (\bord + \3c)
    // Drawing as one polyline with no fill avoids the big solid blobs in the video.
    const path = scaled
      .map((pt, i) => (i === 0 ? `m ${pt.x} ${pt.y}` : `l ${pt.x} ${pt.y}`))
      .join(" ");
    lines.push(
      `Dialogue: 0,${formatAssTime(t0)},${formatAssTime(t1)},Laser,,0,0,0,,{\\an7\\pos(0,0)\\p1\\bord${strokeBord}\\shad0\\1a&HFF&\\3a&H00&\\3c${color}&}${path}{\\p0}`,
    );
  }

  return [
    "[Script Info]",
    "ScriptType: v4.00+",
    `PlayResX: ${width}`,
    `PlayResY: ${height}`,
    "WrapStyle: 0",
    "ScaledBorderAndShadow: yes",
    "YCbCr Matrix: TV.709",
    "",
    "[V4+ Styles]",
    "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding",
    // BorderStyle=1 = outline+shadow (needed for stroke-only drawings)
    "Style: Laser,DejaVu Sans,20,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,0,0,1,0,0,7,0,0,0,1",
    "",
    "[Events]",
    "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ...lines,
    "",
  ].join("\n");
}

function clamp01(n) {
  if (!Number.isFinite(n)) return 0.5;
  return Math.min(1, Math.max(0, n));
}

/** #RRGGBB → ASS &H00BBGGRR */
function hexToAss(hex) {
  const m = String(hex).match(/^#?([0-9a-fA-F]{6})$/);
  if (!m) return "&H00F5C542";
  const rr = m[1].slice(0, 2);
  const gg = m[1].slice(2, 4);
  const bb = m[1].slice(4, 6);
  return `&H00${bb}${gg}${rr}`.toUpperCase();
}

function formatAssTime(sec) {
  const s = Math.max(0, Number(sec) || 0);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const whole = Math.floor(s % 60);
  const cs = Math.min(99, Math.round((s - Math.floor(s)) * 100));
  return `${h}:${String(m).padStart(2, "0")}:${String(whole).padStart(2, "0")}.${String(cs).padStart(2, "0")}`;
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
    durationSec = Math.min(MAX_TOTAL_SEC, Math.max(0.1, durationSec));
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

async function notifyFailure(callbackUrl, callbackSecret, error) {
  if (!callbackUrl) return;
  const headers = { "content-type": "application/json" };
  if (callbackSecret) headers.Authorization = `Bearer ${callbackSecret}`;
  const response = await fetch(callbackUrl, {
    method: "POST",
    headers,
    body: JSON.stringify({ status: "failed", error: String(error || "render failed").slice(0, 1000) }),
  });
  if (!response.ok) throw new Error(`status callback ${response.status}`);
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
