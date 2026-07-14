ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base:3.19
FROM $BUILD_FROM

# Install Node.js and npm
RUN apk add --no-cache nodejs npm

# Set working directory
WORKDIR /usr/src/app

# Copy files
COPY package.json .
COPY index.js .
COPY tuya.js .

# Install dependencies
RUN npm install

# Start script
CMD [ "node", "index.js" ]
