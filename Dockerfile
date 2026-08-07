FROM node:22-bookworm-slim

# ffmpeg for slideshow + voice mix (keep image small for free-tier RAM)
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl fonts-dejavu-core \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY src ./src

ENV PORT=10000
ENV NODE_ENV=production
# Defaults tuned for Render Standard (~2GB). Override in dashboard for free tier.
ENV NODE_OPTIONS=--max-old-space-size=768
ENV RENDER_WIDTH=1280
ENV RENDER_HEIGHT=720
ENV RENDER_FPS=24
ENV RENDER_MAX_SLIDES=40
ENV RENDER_MAX_SEC=180
ENV RENDER_LASERS=1
ENV RENDER_MAX_LASERS=40
ENV FFMPEG_PRESET=veryfast
ENV FFMPEG_CRF=23
EXPOSE 10000

CMD ["node", "src/server.js"]
