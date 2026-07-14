const fs = require("fs");
const path = require("path");
const { exec } = require("child_process");
const net = require("net");
const TuyAPI = require("tuyapi");
const mqtt = require("mqtt");
const express = require("express");
const { TuyaClient } = require("./tuya");

const app = express();
app.use(express.json());

// Configuration file paths
const optionsPath = "/data/options.json";
const localOptionsPath = path.join(__dirname, "options.json");
let configPath = localOptionsPath;

if (fs.existsSync(optionsPath)) {
  configPath = optionsPath;
}

// Manual devices storage path
const manualDevicesPath = fs.existsSync("/data") 
  ? "/data/devices_config.json" 
  : path.join(__dirname, "devices_config.json");

function loadConfig() {
  if (fs.existsSync(configPath)) {
    return JSON.parse(fs.readFileSync(configPath, "utf8"));
  }
  return {};
}

let config = loadConfig();
console.log("[Main] Loaded configuration:", JSON.stringify(config));

// State mapping for active devices
const activeDevices = {};
let syncedDevices = [];
let manualDevices = [];
let localIpMappings = {};
let mqttClient = null;

// Load manual devices from JSON file
function loadManualDevices() {
  try {
    if (fs.existsSync(manualDevicesPath)) {
      return JSON.parse(fs.readFileSync(manualDevicesPath, "utf8"));
    }
  } catch (e) {
    console.error("[Main] Error reading manual devices config:", e.message);
  }
  return [];
}

// Save manual devices to JSON file
function saveManualDevices(list) {
  try {
    fs.writeFileSync(manualDevicesPath, JSON.stringify(list, null, 2), "utf8");
  } catch (e) {
    console.error("[Main] Error saving manual devices config:", e.message);
  }
}

manualDevices = loadManualDevices();

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

// Helper: Scan local IPs
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
    if (!device.local_key) continue;
    for (const ip of openIps) {
      const api = new TuyAPI({
        id: device.id,
        key: device.local_key,
        ip: ip,
        version: "3.5",
        issueRefreshOnConnect: false,
        issueGetOnConnect: false
      });

      api.on("error", () => {});
      try {
        await Promise.race([
          api.connect(),
          new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 800))
        ]);
        mapped[device.id] = { ip, version: "3.5" };
        await api.disconnect();
        console.log(`[Scanner] Mapped ${device.name} (${device.id}) to ${ip} (Version 3.5)`);
        break;
      } catch (err) {
        try { await api.disconnect(); } catch (e) {}

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
            new Promise((_, reject) => setTimeout(() => reject(new Error("Timeout")), 800))
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

// Helper: Scan local IPs or read manual configurations
async function getDeviceIps(devicesToFind) {
  const mapped = {};
  const optionDevices = config.devices || [];

  for (const device of devicesToFind) {
    // Check in HA option configurations first
    let manual = optionDevices.find(d => d.id === device.id);
    // If not in options, check in Ingress Web UI manual configuration
    if (!manual) {
      manual = manualDevices.find(d => d.id === device.id);
    }

    if (manual && manual.ip) {
      mapped[device.id] = { ip: manual.ip, version: "3.5" }; // Default to 3.5
      console.log(`[Config] Manually mapped ${device.name} (${device.id}) to IP: ${manual.ip}`);
    }
  }

  const remaining = devicesToFind.filter(d => !mapped[d.id]);
  if (remaining.length > 0) {
    const scanned = await scanLocalIps(remaining);
    Object.assign(mapped, scanned);
  }

  return mapped;
}

// Fetch devices from cloud
async function fetchCloudDevices() {
  if (!config.clientId || !config.secret || !config.uid) {
    console.warn("[Cloud] Developer credentials missing. Cannot fetch device list.");
    return [];
  }

  const cloudConfig = {
    clientId: config.clientId,
    secret: config.secret,
    baseUrl: config.baseUrl || "https://openapi.tuyain.com",
    uid: config.uid
  };

  const client = new TuyaClient(cloudConfig);
  try {
    console.log(`[Cloud] Fetching device list for UID: ${cloudConfig.uid}...`);
    const res = await client.request({
      method: "GET",
      path: `/v1.0/users/${cloudConfig.uid}/devices`
    });

    if (res.success && Array.isArray(res.result)) {
      const wifiDevices = res.result.filter(d => {
        if (d.sub === true) return false;
        if (d.category && (d.category.startsWith("wg") || d.category === "gwy")) return false;
        return true;
      });
      console.log(`[Cloud] Sync complete. Found ${res.result.length} devices. Filtered to ${wifiDevices.length} Wi-Fi devices.`);
      return wifiDevices;
    } else {
      console.error("[Cloud] Error syncing devices:", res.msg);
    }
  } catch (error) {
    console.error("[Cloud] Exception syncing devices:", error.message);
  }
  return [];
}

// Setup device local loop
function setupDevice(deviceInfo, mapping) {
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

  activeDevices[deviceId] = {
    device,
    info: deviceInfo,
    ip: ip,
    connected: false,
    channels: {},
    dpsState: {}
  };

  registerDeviceEvents(device, deviceInfo);
  connectDevice(device, deviceInfo);
}

function registerDeviceEvents(device, deviceInfo) {
  const deviceId = deviceInfo.id;

  device.on("error", (err) => {
    console.error(`[Tuya Error] Device: ${deviceInfo.name} (${deviceId}):`, err.message);
  });

  device.on("disconnected", () => {
    console.log(`[Tuya Disconnected] Device: ${deviceInfo.name}. Reconnecting in 10s...`);
    activeDevices[deviceId].connected = false;
    setTimeout(() => {
      if (activeDevices[deviceId]) {
        connectDevice(activeDevices[deviceId].device, deviceInfo);
      }
    }, 10000);
  });

  device.on("data", (data) => {
    if (data && data.dps) {
      activeDevices[deviceId].connected = true;
      Object.assign(activeDevices[deviceId].dpsState, data.dps);
      console.log(`[Tuya State] Device: ${deviceInfo.name} (${deviceId}) Data:`, JSON.stringify(data.dps));
      
      handleDeviceState(deviceInfo, data.dps);
    }
  });
}

function handleDeviceState(deviceInfo, dps) {
  const deviceId = deviceInfo.id;
  Object.keys(dps).forEach(key => {
    if (["1", "2", "3", "4"].includes(key) && typeof dps[key] === "boolean") {
      const channelNum = parseInt(key);
      const stateValue = dps[key] ? "ON" : "OFF";
      
      if (!activeDevices[deviceId].channels[channelNum]) {
        registerMqttEntity(deviceInfo, channelNum);
        activeDevices[deviceId].channels[channelNum] = true;
      }

      if (mqttClient && mqttClient.connected) {
        const stateTopic = `homeassistant/switch/${deviceId}_${channelNum}/state`;
        mqttClient.publish(stateTopic, stateValue, { retain: true });
      }
    }
  });
}

async function connectDevice(device, deviceInfo) {
  try {
    await device.connect();
    console.log(`[Tuya Connected] Device: ${deviceInfo.name} (${deviceInfo.id}) locally.`);
    activeDevices[deviceInfo.id].connected = true;
    
    await new Promise(resolve => setTimeout(resolve, 500));
    await device.set({ dps: 1, set: null });
  } catch (err) {
    console.error(`[Tuya Connect Fail] Device: ${deviceInfo.name} (${deviceInfo.id}):`, err.message);
    activeDevices[deviceInfo.id].connected = false;

    if (!activeDevices[deviceInfo.id]) return;

    // Auto version toggle retry logic to handle version mismatch
    const nextVersion = device.device.version === "3.5" ? "3.3" : "3.5";
    console.log(`[Tuya Version Toggle] Retrying connection to ${deviceInfo.name} in 15s using version ${nextVersion}...`);

    try { await device.disconnect(); } catch (e) {}

    const newDevice = new TuyAPI({
      id: deviceInfo.id,
      key: deviceInfo.local_key,
      ip: activeDevices[deviceInfo.id].ip,
      version: nextVersion,
      issueRefreshOnConnect: false,
      issueGetOnConnect: false
    });

    activeDevices[deviceInfo.id].device = newDevice;
    registerDeviceEvents(newDevice, deviceInfo);
    
    setTimeout(() => {
      if (activeDevices[deviceInfo.id]) {
        connectDevice(newDevice, deviceInfo);
      }
    }, 15000);
  }
}

function registerMqttEntity(deviceInfo, channel) {
  if (!mqttClient || !mqttClient.connected) return;

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

// Initialize MQTT broker connection
function connectMqtt() {
  if (!config.mqtt_host) return;

  console.log(`[MQTT] Connecting to broker at mqtt://${config.mqtt_host}:${config.mqtt_port}...`);
  mqttClient = mqtt.connect(`mqtt://${config.mqtt_host}:${config.mqtt_port}`, {
    username: config.mqtt_user || "",
    password: config.mqtt_password || ""
  });

  mqttClient.on("connect", () => {
    console.log("[MQTT] Connected to broker successfully.");
    
    // Subscribe to commands for synced devices
    syncedDevices.forEach(device => {
      const commandTopic = `homeassistant/switch/${device.id}_+/set`;
      mqttClient.subscribe(commandTopic);
    });

    // Subscribe to commands for manual devices
    manualDevices.forEach(device => {
      const commandTopic = `homeassistant/switch/${device.id}_+/set`;
      mqttClient.subscribe(commandTopic);
    });
  });

  mqttClient.on("message", (topic, message) => {
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

// Helper: slugify device name for predicting Entity ID
function getHaEntityId(deviceName, channel) {
  const cleanName = deviceName.toLowerCase()
    .replace(/[^a-z0-9\s-_]/g, "")
    .trim()
    .replace(/[\s-_]+/g, "_");
  return `switch.${cleanName}_switch_${channel}`;
}

// -------------------------------------------------------------
// Web UI & REST API
// -------------------------------------------------------------

app.get("/api/config", (req, res) => {
  res.json(config);
});

app.get("/api/devices", (req, res) => {
  // Merge synced cloud devices and manual devices
  const allDevices = [...syncedDevices];
  
  manualDevices.forEach(m => {
    if (!allDevices.some(d => d.id === m.id)) {
      allDevices.push({
        id: m.id,
        name: m.name,
        product_name: "Manual Tuya Switch",
        local_key: m.local_key,
        isManual: true
      });
    }
  });

  const list = allDevices.map(device => {
    const active = activeDevices[device.id];
    
    // Predict HA entity IDs
    const entities = [];
    const dpsKeys = active ? Object.keys(active.dpsState) : ["1", "2"];
    dpsKeys.forEach(k => {
      if (["1", "2", "3", "4"].includes(k)) {
        entities.push({
          channel: k,
          name: `${device.name || "Tuya Switch"} Switch ${k}`,
          entity_id: getHaEntityId(device.name || "Tuya Switch", k)
        });
      }
    });

    return {
      id: device.id,
      name: device.name || device.product_name,
      model: device.product_name || "Manual Switch",
      local_key: device.local_key,
      ip: active ? active.ip : "Not Found",
      connected: active ? active.connected : false,
      dps: active ? active.dpsState : {},
      isManual: !!device.isManual,
      entities: entities,
      version: active ? active.device.device.version : "3.5"
    };
  });
  res.json(list);
});

app.post("/api/add-device", (req, res) => {
  const { name, id, localKey, ip } = req.body;
  if (!name || !id || !localKey || !ip) {
    return res.status(400).json({ success: false, msg: "All fields are required." });
  }

  // Remove duplicate if exists
  manualDevices = manualDevices.filter(d => d.id !== id);
  manualDevices.push({ name, id, local_key: localKey, ip });
  saveManualDevices(manualDevices);

  console.log(`[UI Action] Manually added device: ${name} (${id}) at ${ip}`);

  // Set up the local client loop instantly
  const deviceInfo = { id, name, local_key: localKey, product_name: "Manual Switch" };
  const mapping = { ip, version: "3.5" };
  
  if (activeDevices[id]) {
    try { activeDevices[id].device.disconnect(); } catch (e) {}
  }
  
  setupDevice(deviceInfo, mapping);

  // Subscribe to MQTT commands for this new device
  if (mqttClient && mqttClient.connected) {
    mqttClient.subscribe(`homeassistant/switch/${id}_+/set`);
  }

  res.json({ success: true, msg: "Device added and connection initialized." });
});

app.post("/api/delete-device", (req, res) => {
  const { id } = req.body;
  if (!id) return res.status(400).json({ success: false, msg: "Device ID required." });

  manualDevices = manualDevices.filter(d => d.id !== id);
  saveManualDevices(manualDevices);

  console.log(`[UI Action] Deleted manual device: ${id}`);

  // Disconnect active loop
  if (activeDevices[id]) {
    try {
      activeDevices[id].device.disconnect();
    } catch (e) {}
    delete activeDevices[id];
  }

  res.json({ success: true, msg: "Device removed." });
});

app.post("/api/control", async (req, res) => {
  const { deviceId, channel, value } = req.body;
  const devInstance = activeDevices[deviceId];
  if (!devInstance || !devInstance.connected) {
    return res.status(400).json({ success: false, msg: "Device not connected locally." });
  }

  try {
    await devInstance.device.set({ dps: parseInt(channel), set: value });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ success: false, msg: err.message });
  }
});

app.get("/", (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>Tuya Local Offline Integration</title>
      <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap" rel="stylesheet">
      <style>
        :root {
          --bg-color: #000000;
          --panel-bg: #09090b;
          --card-bg: #121214;
          --accent: #ffffff;
          --accent-hover: #e4e4e7;
          --text: #f4f4f5;
          --text-muted: #71717a;
          --success: #10b981;
          --danger: #ef4444;
          --border: #27272a;
          --border-hover: #52525b;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
          font-family: 'Outfit', sans-serif;
          background-color: var(--bg-color);
          color: var(--text);
          min-height: 100vh;
          padding: 2rem;
        }
        .container { max-width: 900px; margin: 0 auto; }
        header { margin-bottom: 2rem; display: flex; justify-content: space-between; align-items: center; }
        h1 { font-size: 1.8rem; font-weight: 700; color: var(--accent); letter-spacing: -0.03em; }
        .logo-subtitle { font-size: 0.85rem; color: var(--text-muted); margin-top: 0.2rem; }
        
        .panel {
          background: var(--panel-bg);
          border: 1px solid var(--border);
          border-radius: 12px;
          padding: 1.5rem;
          margin-bottom: 2rem;
        }
        .panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; }
        .panel-title { font-size: 1.1rem; font-weight: 600; color: var(--accent); }

        .device-card {
          background: var(--card-bg);
          border: 1px solid var(--border);
          border-radius: 8px;
          padding: 1.2rem;
          margin-bottom: 1rem;
          transition: border-color 0.2s ease;
          position: relative;
        }
        .device-card:hover { border-color: var(--border-hover); }
        .device-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.6rem; padding-right: 90px; }
        .device-name { font-weight: 600; font-size: 1rem; color: var(--accent); }
        
        .status-badge {
          font-size: 0.7rem; padding: 0.2rem 0.5rem; border-radius: 4px; font-weight: 600; letter-spacing: 0.05em;
        }
        .status-badge.online { background: rgba(16, 185, 129, 0.08); color: var(--success); border: 1px solid rgba(16, 185, 129, 0.2); }
        .status-badge.offline { background: rgba(239, 68, 68, 0.08); color: var(--danger); border: 1px solid rgba(239, 68, 68, 0.2); }

        .device-info { font-size: 0.8rem; color: var(--text-muted); line-height: 1.4; margin-bottom: 0.8rem; }
        .device-footer { display: flex; justify-content: space-between; align-items: center; margin-top: 1rem; border-top: 1px solid var(--border); padding-top: 0.8rem; }

        .more-info-link {
          font-size: 0.75rem; color: var(--text-muted); text-decoration: underline; cursor: pointer; font-weight: 500;
        }
        .more-info-link:hover { color: var(--accent); }

        .entity-toggles { display: flex; flex-wrap: wrap; gap: 0.6rem; }
        .toggle-btn {
          background: transparent;
          border: 1px solid var(--border);
          border-radius: 6px;
          padding: 0.4rem 0.8rem;
          cursor: pointer;
          font-weight: 600;
          font-size: 0.8rem;
          color: var(--text-muted);
          transition: all 0.2s ease;
          display: inline-flex;
          align-items: center;
          gap: 0.5rem;
        }
        .toggle-btn.active {
          background: var(--accent);
          border-color: var(--accent);
          color: var(--bg-color);
        }
        .toggle-btn:hover { border-color: var(--border-hover); }

        .indicator { width: 6px; height: 6px; border-radius: 50%; background: #3f3f46; display: inline-block; }
        .toggle-btn.active .indicator { background: var(--bg-color); }

        .btn-add {
          background: var(--accent);
          border: none; border-radius: 6px; padding: 0.45rem 0.9rem;
          color: var(--bg-color); font-weight: 600; font-size: 0.8rem; cursor: pointer;
          transition: background-color 0.2s;
        }
        .btn-add:hover { background-color: var(--accent-hover); }

        .delete-btn {
          position: absolute; top: 1.2rem; right: 1.2rem;
          background: transparent;
          border: 1px solid rgba(239, 68, 68, 0.3);
          border-radius: 4px; padding: 0.25rem 0.5rem;
          color: var(--danger); font-size: 0.7rem; font-weight: 600; cursor: pointer;
          transition: all 0.2s;
        }
        .delete-btn:hover { background: var(--danger); color: #fff; border-color: var(--danger); }

        /* Modal Overlay & Card styling */
        .modal {
          display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
          background: rgba(0,0,0,0.85);
          justify-content: center; align-items: center; z-index: 1000;
        }
        .modal-content {
          background: var(--panel-bg); border: 1px solid var(--border); border-radius: 8px;
          width: 90%; max-width: 480px; padding: 1.5rem;
        }
        .modal-title { font-size: 1.05rem; font-weight: 700; color: var(--accent); margin-bottom: 1.2rem; display: flex; justify-content: space-between; }
        
        .form-group { margin-bottom: 0.9rem; }
        .form-group label { display: block; font-size: 0.75rem; color: var(--text-muted); margin-bottom: 0.3rem; font-weight: 600; }
        .form-group input {
          width: 100%; background: #000000; border: 1px solid var(--border);
          border-radius: 6px; padding: 0.5rem 0.7rem; color: var(--text); font-family: inherit; font-size: 0.85rem;
        }
        .form-group input:focus { outline: none; border-color: var(--border-hover); }

        .modal-actions { display: flex; gap: 0.6rem; margin-top: 1.5rem; }
        .modal-actions button { flex: 1; padding: 0.5rem; border-radius: 6px; font-weight: 600; cursor: pointer; border: none; font-size: 0.8rem; }
        .btn-save { background: var(--accent); color: var(--bg-color); }
        .btn-cancel { background: transparent; color: var(--text); border: 1px solid var(--border); }

        /* Details list */
        .details-list { font-size: 0.8rem; line-height: 1.6; }
        .details-item { margin-bottom: 0.6rem; border-bottom: 1px solid rgba(255,255,255,0.03); padding-bottom: 0.4rem; }
        .details-label { color: var(--text-muted); font-weight: 600; margin-right: 0.5rem; }
        .details-val { font-family: monospace; color: var(--text); word-break: break-all; }
        
        .code-block {
          background: #000000; border: 1px solid var(--border); border-radius: 4px;
          padding: 0.6rem; font-family: monospace; font-size: 0.75rem; overflow-x: auto; max-height: 120px;
          margin-top: 0.3rem; color: #a1a1aa;
        }
      </style>
    </head>
    <body>
      <div class="container">
        <header>
          <div>
            <h1>Tuya Local Offline</h1>
            <div class="logo-subtitle">Active Ingress Dashboard • Premium Grayscale Style</div>
          </div>
        </header>

        <div class="layout">
          <div class="panel">
            <div class="panel-header">
              <div class="panel-title">Synced LAN Devices</div>
              <button class="btn-add" onclick="openModal()">+ Add Device Manually</button>
            </div>
            <div id="devices-list">
              <div style="text-align: center; color: var(--text-muted); padding: 2rem; font-size: 0.85rem;">Scanning network and loading devices...</div>
            </div>
          </div>
        </div>
      </div>

      <!-- Add Device Modal -->
      <div id="device-modal" class="modal">
        <div class="modal-content">
          <div class="modal-title">
            <span>Add Manual Local Device</span>
            <span style="cursor:pointer;" onclick="closeModal()">&times;</span>
          </div>
          <form id="device-form">
            <div class="form-group">
              <label>Device Name (e.g. Wifi Switch)</label>
              <input type="text" id="devName" placeholder="My Switch" required>
            </div>
            <div class="form-group">
              <label>Device ID</label>
              <input type="text" id="devId" placeholder="d7cdb16228c510..." required>
            </div>
            <div class="form-group">
              <label>Local Key</label>
              <input type="text" id="devKey" placeholder="Voj9A=xehh..." required>
            </div>
            <div class="form-group">
              <label>Local IP Address</label>
              <input type="text" id="devIp" placeholder="192.168.2.18" required>
            </div>
            <div class="modal-actions">
              <button type="button" class="btn-cancel" onclick="closeModal()">Cancel</button>
              <button type="submit" class="btn-save">Save & Connect</button>
            </div>
          </form>
        </div>
      </div>

      <!-- More Info Modal -->
      <div id="info-modal" class="modal">
        <div class="modal-content" style="max-width: 520px;">
          <div class="modal-title">
            <span>Device Integration Info</span>
            <span style="cursor:pointer;" onclick="closeInfoModal()">&times;</span>
          </div>
          <div class="details-list" id="info-details">
            <!-- Populated dynamically -->
          </div>
          <div class="modal-actions">
            <button class="btn-cancel" onclick="closeInfoModal()">Close</button>
          </div>
        </div>
      </div>

      <script>
        let cachedDevicesList = [];

        function openModal() {
          document.getElementById("device-modal").style.display = "flex";
        }
        function closeModal() {
          document.getElementById("device-modal").style.display = "none";
          document.getElementById("device-form").reset();
        }

        function openInfoModal(deviceId) {
          const dev = cachedDevicesList.find(d => d.id === deviceId);
          if (!dev) return;

          const container = document.getElementById("info-details");
          
          let entitiesHtml = "";
          if (dev.entities && dev.entities.length > 0) {
            dev.entities.forEach(ent => {
              entitiesHtml += \`
                <div style="margin-bottom: 0.5rem; padding-left: 0.5rem; border-left: 1px solid var(--border);">
                  <div style="font-weight:600; color:var(--accent);">\${ent.name}</div>
                  <div style="color:var(--text-muted); font-size:0.75rem; font-family:monospace;">\${ent.entity_id}</div>
                </div>
              \`;
            });
          } else {
            entitiesHtml = '<div style="color:var(--text-muted); font-size:0.75rem;">No Home Assistant entities mapped yet. Connection is required.</div>';
          }

          container.innerHTML = \`
            <div class="details-item">
              <span class="details-label">Device Name:</span>
              <span class="details-val">\${dev.name}</span>
            </div>
            <div class="details-item">
              <span class="details-label">Device ID:</span>
              <span class="details-val">\${dev.id}</span>
            </div>
            <div class="details-item">
              <span class="details-label">Local IP Address:</span>
              <span class="details-val">\${dev.ip}</span>
            </div>
            <div class="details-item">
              <span class="details-label">Local Key:</span>
              <span class="details-val">\${dev.local_key}</span>
            </div>
            <div class="details-item">
              <span class="details-label">Protocol Version:</span>
              <span class="details-val">\${dev.version || '3.5'}</span>
            </div>
            <div class="details-item" style="border-bottom:none;">
              <span class="details-label">Registered HA Entities:</span>
              <div style="margin-top:0.4rem;">\${entitiesHtml}</div>
            </div>
            <div class="details-item" style="border-bottom:none;">
              <span class="details-label">Raw DPS State:</span>
              <pre class="code-block">\${JSON.stringify(dev.dps, null, 2)}</pre>
            </div>
          \`;
          
          document.getElementById("info-modal").style.display = "flex";
        }

        function closeInfoModal() {
          document.getElementById("info-modal").style.display = "none";
        }

        // Add device form submit
        document.getElementById("device-form").addEventListener("submit", (e) => {
          e.preventDefault();
          const body = {
            name: document.getElementById("devName").value.trim(),
            id: document.getElementById("devId").value.trim(),
            localKey: document.getElementById("devKey").value.trim(),
            ip: document.getElementById("devIp").value.trim()
          };

          fetch("api/add-device", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body)
          })
          .then(r => r.json())
          .then(res => {
            if (res.success) {
              closeModal();
              refreshDevices();
            } else {
              alert("Error: " + res.msg);
            }
          });
        });

        // Load devices list and poll
        function refreshDevices() {
          fetch("api/devices")
            .then(r => r.json())
            .then(devices => {
              cachedDevicesList = devices;
              const container = document.getElementById("devices-list");
              if (devices.length === 0) {
                container.innerHTML = '<div style="text-align: center; color: var(--text-muted); padding: 2rem; font-size: 0.85rem;">No devices loaded. Click "+ Add Device Manually"!</div>';
                return;
              }

              let html = "";
              devices.forEach(d => {
                const isOnline = d.connected;
                const statusText = isOnline ? "ONLINE (LAN)" : "OFFLINE (LAN)";
                const badgeClass = isOnline ? "status-badge online" : "status-badge offline";
                
                // Expose switches for DPS 1, 2, 3, 4
                let togglesHtml = "";
                let hasToggles = false;
                
                ["1", "2", "3", "4"].forEach(key => {
                  if (d.dps[key] !== undefined && typeof d.dps[key] === "boolean") {
                    hasToggles = true;
                    const isActive = d.dps[key];
                    const activeClass = isActive ? "active" : "";
                    togglesHtml += \`
                      <div class="toggle-btn \${activeClass}" onclick="toggleRelay('\${d.id}', \${key}, \${!isActive})">
                        <span>Relay \${key}</span>
                        <div class="indicator"></div>
                      </div>
                    \`;
                  }
                });

                if (!hasToggles) {
                  togglesHtml = '<div style="font-size: 0.8rem; color: var(--text-muted);">No active switch relays detected yet. Connecting...</div>';
                }

                // Delete button only for manual devices
                const deleteHtml = d.isManual 
                  ? \`<button class="delete-btn" onclick="deleteDevice('\${d.id}')">Delete</button>\`
                  : "";

                html += \`
                  <div class="device-card">
                    \${deleteHtml}
                    <div class="device-header">
                      <div class="device-name">\${d.name}</div>
                      <span class="\${badgeClass}">\${statusText}</span>
                    </div>
                    <div class="device-info">
                      <strong>IP:</strong> \${d.ip} &nbsp;|&nbsp;
                      <strong>Device ID:</strong> \${d.id} &nbsp;|&nbsp;
                      <strong>Key:</strong> \${d.local_key}
                    </div>
                    <div class="device-footer">
                      <span class="more-info-link" onclick="openInfoModal('\${d.id}')">More Info</span>
                      <div class="entity-toggles">
                        \${togglesHtml}
                      </div>
                    </div>
                  </div>
                \`;
              });

              container.innerHTML = html;
            })
            .catch(err => console.error("Error refreshing devices:", err));
        }

        function toggleRelay(deviceId, channel, value) {
          fetch("api/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ deviceId, channel, value })
          })
          .then(r => r.json())
          .then(res => {
            if (res.success) {
              refreshDevices();
            } else {
              alert("Control Failed: " + res.msg);
            }
          });
        }

        function deleteDevice(deviceId) {
          if (confirm("Are you sure you want to remove this device?")) {
            fetch("api/delete-device", {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify({ id: deviceId })
            })
            .then(r => r.json())
            .then(res => {
              if (res.success) {
                refreshDevices();
              } else {
                alert("Failed to delete device.");
              }
            });
          }
        }

        // Start polling
        refreshDevices();
        setInterval(refreshDevices, 2500);
      </script>
    </body>
    </html>
  `);
});

// Start Express Web Server on port 8099
const PORT = 8099;
app.listen(PORT, () => {
  console.log(`[Web Server] Ingress web UI running on port ${PORT}`);
});

// Start main control loop
async function runLoops() {
  syncedDevices = await fetchCloudDevices();
  
  // Merge cloud devices and manual devices to resolve local IP mappings
  const allDevices = [...syncedDevices];
  manualDevices.forEach(m => {
    if (!allDevices.some(d => d.id === m.id)) {
      allDevices.push({
        id: m.id,
        name: m.name,
        local_key: m.local_key,
        product_name: "Manual Switch"
      });
    }
  });

  if (allDevices.length > 0) {
    localIpMappings = await getDeviceIps(allDevices);
    connectMqtt();
    
    allDevices.forEach(device => {
      const mapping = localIpMappings[device.id];
      if (mapping) {
        setupDevice(device, mapping);
      } else {
        console.warn(`[Main] Local IP mapping failed for ${device.name} (${device.id})`);
      }
    });
  } else {
    console.log("[Main] No devices loaded. Web UI is active. Please add devices manually or configure cloud settings.");
  }
}

runLoops();
