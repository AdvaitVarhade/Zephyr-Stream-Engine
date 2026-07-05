FROM node:latest

WORKDIR /app
COPY . ./

CMD ["sh", "-c", "node server.js & sleep 2 && node client.js"]
