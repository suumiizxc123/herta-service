FROM node:22-alpine
WORKDIR /app
COPY platform/worker/package.json ./package.json
RUN apk add --no-cache postgresql-client
RUN npm install --omit=dev --no-audit --no-fund
RUN mkdir -p /data /backups && chown -R node:node /app /data /backups
COPY platform/worker/server.js ./platform/worker/server.js
COPY platform/license-client.js ./platform/license-client.js
COPY electron/source-files.js ./electron/source-files.js
COPY electron/backup-archive.js ./electron/backup-archive.js
COPY electron/herta-auth.js ./electron/herta-auth.js
COPY electron/cron.js ./electron/cron.js
COPY electron/http-headers.js ./electron/http-headers.js
USER node
ENV NODE_ENV=production
EXPOSE 8127
CMD ["node", "platform/worker/server.js"]
