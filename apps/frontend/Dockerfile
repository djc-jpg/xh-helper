FROM node:20-alpine

WORKDIR /app

COPY apps/frontend/package.json /app/package.json
COPY apps/frontend/package-lock.json /app/package-lock.json
RUN npm ci

COPY apps/frontend /app

EXPOSE 3000

CMD ["npm", "run", "dev", "--", "-H", "0.0.0.0", "-p", "3000"]
