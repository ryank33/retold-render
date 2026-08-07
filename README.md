# Retold Render

The ffmpeg container for Retold's quick-share videos. It runs as the existing `retold-render`
Render web service and accepts both the legacy Retold payload and the current signed Worker
manifest.

## Current output

- 1280×720 H.264 MP4 at 24 fps
- AAC mono voice mix, with each independently recorded span placed on the Worker master clock
- Up to six participant tracks
- Timed slide changes
- Laser spotlight and draw overlays burned into the video
- Authenticated streaming callback to the Worker, which stores the MP4 in R2

## Required environment

```text
RENDER_HOOK_KEY=<same secret configured on the Worker>
```

Resolution, frame rate, limits, laser rendering, ffmpeg preset, and CRF are configurable with the
`RENDER_*` and `FFMPEG_*` environment variables used in `src/server.js`.

## Local run

```bash
npm install
npm start
curl http://localhost:10000/health
```

Production uses the included Dockerfile so ffmpeg and libass are available consistently.
