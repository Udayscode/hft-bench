# =========================================================
# BUILD STAGE
# =========================================================
FROM node:20-alpine AS builder

WORKDIR /app

# Enable npm dependency cache
COPY package*.json ./
RUN npm ci

COPY . .

# Build Next.js App (generates .next/standalone if configured)
RUN npm run build

# =========================================================
# RUNTIME STAGE
# =========================================================
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

# Copy standalone build assets
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

EXPOSE 3000

CMD ["node", "server.js"]