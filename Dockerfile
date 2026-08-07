FROM node:22-bookworm-slim

# ffmpeg for slideshow + voice mix (keep image small for free-tier RAM)
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY src ./src

ENV PORT=10000
ENV NODE_ENV=production
# Cap Node heap so the process fails soft before the whole container is OOM-killed
ENV NODE_OPTIONS=--max-old-space-size=192
# Free-tier safe encode defaults (override in Render dashboard if you upgrade)
ENV RENDER_WIDTH=854
ENV RENDER_HEIGHT=480
ENV RENDER_FPS=15
ENV RENDER_MAX_SLIDES=20
ENV RENDER_MAX_SEC=90
ENV RENDER_LASERS=0
ENV FFMPEG_PRESET=ultrafast
ENV FFMPEG_CRF=30
EXPOSE 10000

CMD ["node", "src/server.js"]
