const fs = require("fs");
const path = require("path");
const { exec } = require("child_process");
const net = require("net");
const TuyAPI = require("tuyapi");
const mqtt = require("mqtt");
const { TuyaClient } = require("./tuya");

// Load Home Assistant Options or fallback to local options.json for development
let config = {};
const optionsPath = "/data/options.json";
const localOptionsPath = path.join(__dirname, "options.json");

if (fs.existsSync(optionsPath)) {
  config = JSON.parse(fs.readFileSync(optionsPath, "utf8"));
  console.log("[Main] Loaded configuration from Home Assistant options.");
} else if (fs.existsSync(localOptionsPath)) {
  config = JSON.parse(fs.readFileSync(localOptionsPath, "utf8"));
  console.log("[Main] Loaded configuration from local options.json (Dev Mode).");
} else {
  console.error("[Main] Configuration file not found! Exiting.");
  process.exit(1);
}

const cloudConfig = {
  clientId: config.clientId,
  secret: config.secret,
  baseUrl: config.baseUrl || "https://openapi.tuyain.com",
  uid: config.uid
};

const mqttConfig = {
  host: config.mqtt_host || "localhost",
  port: config.mqtt_port || 1883,
  username: config.mqtt_user || "",
  password: config.mqtt_password || ""
};

// State mapping to store active TuyAPI instances and their connections
const activeDevices = {};

// Helper: Get all active IPs from the local ARP table
function getArpIps() {
  return new Promise((resolve) => {
    exec("arp -a", (err, stdout) => {
      if (err) return resolve([]);
      const ips = [];
      const lines = stdout.split("\n");
      for (const line of lines) {
        const match = line.trim().match(/^((?:192\.168|10\.\d+|172\.\d+)\.\d+\.\d+)\s+/);
        if (match) {
          ips.push(match[1]);
        }
      }
      resolve(ips);
    });
  });
}

// Helper: Check if port 6668 is open on a host
function checkPort(ip, port = 6668, timeout = 400) {
  return new Promise((resolve) => {
    const socket = new net.Socket();
    let resolved = false;
    socket.setTimeout(timeout);
    socket.connect(port, ip, () => {
      socket.destroy();
      if (!resolved) { resolved = true; resolve(true); }
    });
    const fail = () => {
      socket.destroy();
      if (!resolved) { resolved = true; resolve(false); }
    };
    socket.on("error", fail);
    socket.on("timeout", fail);
  });
}

// Helper: Perform quick local IP scanning to map device IDs to IPs
async function scanLocalIps(devicesToFind) {
  console.log("[Scanner] Scanning local network IPs...");
  const arpIps = await getArpIps();
  const openIps = [];
  
  for (const ip of arpIps) {
    if (await checkPort(ip)) {
      openIps.push(ip);
    }
  }

  const mapped = {};
  console.log(`[Scanner] Found ${openIps.length} active Tuya local hosts. Verifying signatures...`);

  for (const device of devicesToFind) {
    for (const ip of openIps) {
      // Try to connect briefly to verify if this ID belongs to the IP
      const api = new TuyAPI({
        id: device.id,
        key: device.local_key,
        ip: ip,
        version: "3.5", // Start with 3.5 negotiation
        issueRefreshOnConnect: false,
        issueGetOnConnect: false
      });

      api.on("error", () => {});
      try {
        await Promise.race([
          api.connect(),
          new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 1000))
        ]);
        // Successfully connected!
        mapped[device.id] = { ip, version: "3.5" };
        await api.disconnect();
        console.log(`[Scanner] Mapped ${device.name} (${device.id}) to ${ip} (Version 3.5)`);
        break;
      } catch (err) {
        try { await api.disconnect(); } catch (e) {}

        // Retry with 3.3
        const api33 = new TuyAPI({
          id: device.id,
          key: device.local_key,
          ip: ip,
          version: "3.3",
          issueRefreshOnConnect: false,
          issueGetOnConnect: false
        });
        api33.on("error", () => {});
        try {
          await Promise.race([
            api33.connect(),
            new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 1000))
          ]);
          mapped[device.id] = { ip, version: "3.3" };
          await api33.disconnect();
          console.log(`[Scanner] Mapped ${device.name} (${device.id}) to ${ip} (Version 3.3)`);
          break;
        } catch (err33) {
          try { await api33.disconnect(); } catch (e) {}
        }
      }
    }
  }
  return mapped;
}

// Fetch devices from Tuya Cloud
async function fetchCloudDevices() {
  const client = new TuyaClient(cloudConfig);
  try {
    console.log(`[Cloud] Fetching device list for UID: ${cloudConfig.uid}...`);
    const res = await client.request({
      method: "GET",
      path: `/v1.0/users/${cloudConfig.uid}/devices`
    });

    if (res.success && Array.isArray(res.result)) {
      console.log(`[Cloud] Sync complete. Found ${res.result.length} devices.`);
      return res.result;
    } else {
      console.error("[Cloud] Error syncing devices:", res.msg);
    }
  } catch (error) {
    console.error("[Cloud] Exception syncing devices:", error.message);
  }
  return [];
}

async function main() {
  // 1. Fetch devices from cloud
  const devices = await fetchCloudDevices();
  if (devices.length === 0) {
    console.error("[Main] No devices found. Exiting.");
    process.exit(1);
  }

  // 2. Discover local IPs
  const ipMapping = await scanLocalIps(devices);

  // 3. Connect to MQTT Broker
  console.log(`[MQTT] Connecting to broker at mqtt://${mqttConfig.host}:${mqttConfig.port}...`);
  const mqttClient = mqtt.connect(`mqtt://${mqttConfig.host}:${mqttConfig.port}`, {
    username: mqttConfig.username,
    password: mqttConfig.password
  });

  mqttClient.on("connect", () => {
    console.log("[MQTT] Connected to broker successfully.");
    
    // Subscribe to all command topics for our devices
    devices.forEach(device => {
      const commandTopic = `homeassistant/switch/${device.id}_+/set`;
      mqttClient.subscribe(commandTopic);
      console.log(`[MQTT] Subscribed to command topic: ${commandTopic}`);
    });

    // Start local device loops
    devices.forEach(device => {
      const mapping = ipMapping[device.id];
      if (!mapping) {
        console.warn(`[Main] Warning: Could not find local IP for device ${device.name} (${device.id}). Offline control unavailable.`);
        return;
      }

      setupDevice(device, mapping, mqttClient);
    });
  });

  mqttClient.on("message", (topic, message) => {
    // Topic format: homeassistant/switch/{device_id}_{channel}/set
    const match = topic.match(/homeassistant\/switch\/([a-zA-Z0-9]+)_(\d+)\/set/);
    if (match) {
      const deviceId = match[1];
      const channel = match[2];
      const command = message.toString().toUpperCase(); // ON or OFF

      const devInstance = activeDevices[deviceId];
      if (devInstance) {
        const value = command === "ON";
        console.log(`[MQTT Command] Device: ${deviceId}, Channel: ${channel} -> ${command}`);
        
        devInstance.device.set({ dps: parseInt(channel), set: value }).catch(err => {
          console.error(`[Tuya Control] Failed to set DPS ${channel} on device ${deviceId}:`, err.message);
        });
      }
    }
  });
}

function setupDevice(deviceInfo, mapping, mqttClient) {
  const deviceId = deviceInfo.id;
  const localKey = deviceInfo.local_key;
  const ip = mapping.ip;
  const version = mapping.version;

  console.log(`[Tuya] Initializing connection to ${deviceInfo.name} at ${ip} (v${version})...`);

  const device = new TuyAPI({
    id: deviceId,
    key: localKey,
    ip: ip,
    version: version,
    issueRefreshOnConnect: false,
    issueGetOnConnect: false
  });

  activeDevices[deviceId] = { device, info: deviceInfo, channels: {} };

  device.on("error", (err) => {
    console.error(`[Tuya Error] Device: ${deviceInfo.name} (${deviceId}):`, err.message);
  });

  device.on("disconnected", () => {
    console.log(`[Tuya Disconnected] Device: ${deviceInfo.name}. Retrying in 10s...`);
    setTimeout(() => connectDevice(device, deviceInfo, mqttClient), 10000);
  });

  device.on("data", (data) => {
    if (data && data.dps) {
      console.log(`[Tuya State] Device: ${deviceInfo.name} (${deviceId}) Data:`, JSON.stringify(data.dps));
      
      // Auto-discover channels from the payload keys (look for keys "1", "2", "3", "4" that contain booleans)
      Object.keys(data.dps).forEach(key => {
        if (["1", "2", "3", "4"].includes(key) && typeof data.dps[key] === "boolean") {
          const channelNum = parseInt(key);
          const stateValue = data.dps[key] ? "ON" : "OFF";
          
          // If we haven't registered this channel yet, send discovery payload first
          if (!activeDevices[deviceId].channels[channelNum]) {
            registerMqttEntity(deviceInfo, channelNum, mqttClient);
            activeDevices[deviceId].channels[channelNum] = true;
          }

          // Publish state update
          const stateTopic = `homeassistant/switch/${deviceId}_${channelNum}/state`;
          mqttClient.publish(stateTopic, stateValue, { retain: true });
        }
      });
    }
  });

  connectDevice(device, deviceInfo, mqttClient);
}

async function connectDevice(device, deviceInfo, mqttClient) {
  try {
    await device.connect();
    console.log(`[Tuya Connected] Device: ${deviceInfo.name} (${deviceInfo.id}) locally.`);
    
    // Perform standard set-null query to fetch initial status and discover channels
    await new Promise(resolve => setTimeout(resolve, 500));
    await device.set({ dps: 1, set: null });
  } catch (err) {
    console.error(`[Tuya Connect Fail] Device: ${deviceInfo.name} (${deviceInfo.id}):`, err.message);
    console.log(`[Tuya Connect Fail] Retrying connection to ${deviceInfo.name} in 15s...`);
    setTimeout(() => connectDevice(device, deviceInfo, mqttClient), 15000);
  }
}

function registerMqttEntity(deviceInfo, channel, mqttClient) {
  const deviceId = deviceInfo.id;
  const modelName = deviceInfo.product_name || "Tuya Multi-Channel Switch";
  const entityName = `${deviceInfo.name} Switch ${channel}`;
  const configTopic = `homeassistant/switch/${deviceId}_${channel}/config`;

  const discoveryPayload = {
    name: entityName,
    state_topic: `homeassistant/switch/${deviceId}_${channel}/state`,
    command_topic: `homeassistant/switch/${deviceId}_${channel}/set`,
    payload_on: "ON",
    payload_off: "OFF",
    unique_id: `tuya_${deviceId}_${channel}`,
    device: {
      identifiers: [`tuya_${deviceId}`],
      name: deviceInfo.name,
      model: modelName,
      manufacturer: "Tuya"
    }
  };

  console.log(`[MQTT Discovery] Registering Entity: "${entityName}" (DPS: ${channel})`);
  mqttClient.publish(configTopic, JSON.stringify(discoveryPayload), { retain: true });
}

// Start main execution
main();
