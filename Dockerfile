FROM node:22-bookworm-slim

# ffmpeg for silent slideshow / mixed audio later
RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY src ./src

ENV PORT=10000
ENV NODE_ENV=production
EXPOSE 10000

CMD ["npm", "start"]
