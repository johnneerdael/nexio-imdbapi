FROM node:22-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps/web/package.json apps/web/package.json
RUN npm ci

COPY apps/web apps/web
COPY docs docs

ARG API_BASE_URL=http://api:8080
ENV API_BASE_URL=${API_BASE_URL}

RUN npm run build:docs && npm run build:web

FROM node:22-alpine AS runtime

WORKDIR /app/apps/web
ENV NODE_ENV=production

COPY --from=builder /app/apps/web/.output ./.output
COPY --from=builder /app/docs /app/docs
COPY --from=builder /app/apps/web/package.json ./package.json

EXPOSE 3000
CMD ["node", ".output/server/index.mjs"]
