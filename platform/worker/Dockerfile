FROM node:22-alpine
WORKDIR /app
COPY platform/worker/package.json ./package.json
RUN npm install --omit=dev --no-audit --no-fund
RUN mkdir -p /data /backups && chown -R node:node /app /data /backups
COPY platform/worker/server.js ./platform/worker/server.js
COPY electron/source-files.js ./electron/source-files.js
COPY electron/backup-archive.js ./electron/backup-archive.js
COPY electron/cron.js ./electron/cron.js
USER node
ENV NODE_ENV=production
EXPOSE 8127
CMD ["node", "platform/worker/server.js"]
