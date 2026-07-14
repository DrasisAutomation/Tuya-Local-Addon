FROM node:20-alpine

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
