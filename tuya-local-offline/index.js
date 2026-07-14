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
    return {
      id: device.id,
      name: device.name || device.product_name,
      model: device.product_name || "Manual Switch",
      local_key: device.local_key,
      ip: active ? active.ip : "Not Found",
      connected: active ? active.connected : false,
      dps: active ? active.dpsState : {},
      isManual: !!device.isManual
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
          --bg-color: #0b0f19;
          --panel-bg: rgba(20, 27, 45, 0.7);
          --accent: #00f2fe;
          --accent-hover: #4facfe;
          --text: #f3f4f6;
          --text-muted: #9ca3af;
          --success: #10b981;
          --danger: #ef4444;
          --border: rgba(0, 242, 254, 0.15);
        }
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body {
          font-family: 'Outfit', sans-serif;
          background-color: var(--bg-color);
          background-image: radial-gradient(circle at 10% 20%, rgba(0, 242, 254, 0.05) 0%, transparent 40%),
                            radial-gradient(circle at 90% 80%, rgba(79, 172, 254, 0.05) 0%, transparent 40%);
          color: var(--text);
          min-height: 100vh;
          padding: 2rem;
        }
        .container { max-width: 1000px; margin: 0 auto; }
        header { margin-bottom: 2rem; display: flex; justify-content: space-between; align-items: center; }
        h1 { font-size: 2rem; font-weight: 700; background: linear-gradient(135deg, var(--accent), var(--accent-hover)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .logo-subtitle { font-size: 0.9rem; color: var(--text-muted); margin-top: 0.2rem; }
        
        .panel {
          background: var(--panel-bg);
          backdrop-filter: blur(12px);
          border: 1px solid var(--border);
          border-radius: 16px;
          padding: 1.5rem;
          box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
          margin-bottom: 2rem;
        }
        .panel-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.2rem; border-bottom: 1px solid var(--border); padding-bottom: 0.5rem; }
        .panel-title { font-size: 1.25rem; font-weight: 600; color: var(--accent); }

        .device-card {
          background: rgba(255, 255, 255, 0.02);
          border: 1px solid rgba(255, 255, 255, 0.05);
          border-radius: 12px;
          padding: 1.2rem;
          margin-bottom: 1rem;
          transition: all 0.3s ease;
          position: relative;
        }
        .device-card:hover { border-color: var(--accent); box-shadow: 0 0 15px rgba(0, 242, 254, 0.1); }
        .device-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.8rem; padding-right: 80px; }
        .device-name { font-weight: 600; font-size: 1.1rem; }
        
        .status-badge {
          font-size: 0.75rem; padding: 0.25rem 0.6rem; border-radius: 99px; font-weight: 600;
        }
        .status-badge.online { background: rgba(16, 185, 129, 0.15); color: var(--success); border: 1px solid var(--success); }
        .status-badge.offline { background: rgba(239, 68, 68, 0.15); color: var(--danger); border: 1px solid var(--danger); }

        .device-info { font-size: 0.85rem; color: var(--text-muted); line-height: 1.4; margin-bottom: 1rem; }

        .entity-toggles { display: grid; grid-template-columns: repeat(auto-fill, minmax(130px, 1fr)); gap: 0.8rem; }
        .toggle-btn {
          background: rgba(255, 255, 255, 0.05);
          border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 8px;
          padding: 0.6rem;
          text-align: center;
          cursor: pointer;
          font-weight: 600;
          font-size: 0.9rem;
          transition: all 0.2s ease;
          display: flex;
          justify-content: space-between;
          align-items: center;
        }
        .toggle-btn.active {
          background: linear-gradient(135deg, rgba(0, 242, 254, 0.2), rgba(79, 172, 254, 0.2));
          border-color: var(--accent);
          color: var(--accent);
          box-shadow: 0 0 10px rgba(0, 242, 254, 0.15);
        }
        .toggle-btn:hover { background: rgba(255, 255, 255, 0.08); }
        .indicator { width: 10px; height: 10px; border-radius: 50%; background: #374151; display: inline-block; }
        .toggle-btn.active .indicator { background: var(--accent); box-shadow: 0 0 8px var(--accent); }

        .btn-add {
          background: linear-gradient(135deg, var(--accent), var(--accent-hover));
          border: none; border-radius: 8px; padding: 0.5rem 1rem;
          color: #0b0f19; font-weight: 700; font-size: 0.85rem; cursor: pointer;
          transition: opacity 0.2s;
        }
        .btn-add:hover { opacity: 0.9; }

        .delete-btn {
          position: absolute; top: 1.2rem; right: 1.2rem;
          background: rgba(239, 68, 68, 0.1);
          border: 1px solid rgba(239, 68, 68, 0.2);
          border-radius: 6px; padding: 0.35rem 0.6rem;
          color: var(--danger); font-size: 0.75rem; font-weight: 600; cursor: pointer;
          transition: all 0.2s;
        }
        .delete-btn:hover { background: var(--danger); color: #fff; border-color: var(--danger); }

        /* Modal Overlay & Card styling */
        .modal {
          display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
          background: rgba(0,0,0,0.6); backdrop-filter: blur(4px);
          justify-content: center; align-items: center; z-index: 1000;
        }
        .modal-content {
          background: #141b2d; border: 1px solid var(--border); border-radius: 16px;
          width: 90%; max-width: 450px; padding: 1.5rem; box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.5);
        }
        .modal-title { font-size: 1.2rem; font-weight: 700; color: var(--accent); margin-bottom: 1.2rem; display: flex; justify-content: space-between; }
        
        .form-group { margin-bottom: 1rem; }
        .form-group label { display: block; font-size: 0.8rem; color: var(--text-muted); margin-bottom: 0.35rem; font-weight: 600; }
        .form-group input {
          width: 100%; background: rgba(0, 0, 0, 0.2); border: 1px solid rgba(255, 255, 255, 0.1);
          border-radius: 8px; padding: 0.6rem; color: var(--text); font-family: inherit; font-size: 0.9rem;
        }
        .form-group input:focus { outline: none; border-color: var(--accent); }

        .modal-actions { display: flex; gap: 0.8rem; margin-top: 1.5rem; }
        .modal-actions button { flex: 1; padding: 0.6rem; border-radius: 8px; font-weight: 600; cursor: pointer; border: none; }
        .btn-save { background: linear-gradient(135deg, var(--accent), var(--accent-hover)); color: #0b0f19; }
        .btn-cancel { background: rgba(255,255,255,0.08); color: var(--text); border: 1px solid rgba(255,255,255,0.1); }
      </style>
    </head>
    <body>
      <div class="container">
        <header>
          <div>
            <h1>Tuya Local Offline Integration</h1>
            <div class="logo-subtitle">Active Ingress Control Dashboard • Dark Aesthetics</div>
          </div>
        </header>

        <div class="layout">
          <div class="panel">
            <div class="panel-header">
              <div class="panel-title">Synced LAN Devices</div>
              <button class="btn-add" onclick="openModal()">+ Add Device Manually</button>
            </div>
            <div id="devices-list">
              <div style="text-align: center; color: var(--text-muted); padding: 2rem;">Scanning network and loading devices...</div>
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

      <script>
        function openModal() {
          document.getElementById("device-modal").style.display = "flex";
        }
        function closeModal() {
          document.getElementById("device-modal").style.display = "none";
          document.getElementById("device-form").reset();
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
              const container = document.getElementById("devices-list");
              if (devices.length === 0) {
                container.innerHTML = '<div style="text-align: center; color: var(--text-muted); padding: 2rem;">No devices loaded. Use cloud config or click "+ Add Device Manually"!</div>';
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
                    togglesHtml += `
                      <div class="toggle-btn \${activeClass}" onclick="toggleRelay('\${d.id}', \${key}, \${!isActive})">
                        <span>Relay \${key}</span>
                        <div class="indicator"></div>
                      </div>
                    `;
                  }
                });

                if (!hasToggles) {
                  togglesHtml = '<div style="font-size: 0.85rem; color: var(--text-muted);">No active switch relays detected yet. Connecting...</div>';
                }

                // Delete button only for manual devices
                const deleteHtml = d.isManual 
                  ? `<button class="delete-btn" onclick="deleteDevice('\${d.id}')">Delete</button>`
                  : "";

                html += `
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
                    <div class="entity-toggles">
                      \${togglesHtml}
                    </div>
                  </div>
                `;
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
