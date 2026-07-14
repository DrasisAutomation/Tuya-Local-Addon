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
let localIpMappings = {};
let mqttClient = null;

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
  const manualDevices = config.devices || [];

  for (const device of devicesToFind) {
    const manual = manualDevices.find(d => d.id === device.id);
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
    setTimeout(() => connectDevice(activeDevices[deviceId].device, deviceInfo), 10000);
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
    
    setTimeout(() => connectDevice(newDevice, deviceInfo), 15000);
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
    
    syncedDevices.forEach(device => {
      const commandTopic = `homeassistant/switch/${device.id}_+/set`;
      mqttClient.subscribe(commandTopic);
      console.log(`[MQTT] Subscribed to command topic: ${commandTopic}`);
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

app.post("/api/config", (req, res) => {
  const newConfig = req.body;
  try {
    fs.writeFileSync(configPath, JSON.stringify(newConfig, null, 2), "utf8");
    config = newConfig;
    res.json({ success: true, msg: "Configuration saved successfully. Restarting service..." });
    
    setTimeout(() => {
      console.log("[Main] Configuration changed. Exiting process for reboot.");
      process.exit(0);
    }, 1000);
  } catch (err) {
    res.status(500).json({ success: false, msg: err.message });
  }
});

app.get("/api/devices", (req, res) => {
  const list = syncedDevices.map(device => {
    const active = activeDevices[device.id];
    return {
      id: device.id,
      name: device.name || device.product_name,
      model: device.product_name,
      local_key: device.local_key,
      ip: active ? active.ip : "Not Found",
      connected: active ? active.connected : false,
      dps: active ? active.dpsState : {}
    };
  });
  res.json(list);
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
        .container { max-width: 1100px; margin: 0 auto; }
        header { margin-bottom: 2rem; display: flex; justify-content: space-between; align-items: center; }
        h1 { font-size: 2rem; font-weight: 700; background: linear-gradient(135deg, var(--accent), var(--accent-hover)); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .logo-subtitle { font-size: 0.9rem; color: var(--text-muted); margin-top: 0.2rem; }
        
        .layout { display: grid; grid-template-columns: 1fr; gap: 2rem; }
        @media(max-width: 900px) { .layout { grid-template-columns: 1fr; } }

        .panel {
          background: var(--panel-bg);
          backdrop-filter: blur(12px);
          border: 1px solid var(--border);
          border-radius: 16px;
          padding: 1.5rem;
          box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        }
        .panel-title { font-size: 1.25rem; font-weight: 600; margin-bottom: 1.2rem; border-bottom: 1px solid var(--border); padding-bottom: 0.5rem; color: var(--accent); }

        .device-card {
          background: rgba(255, 255, 255, 0.02);
          border: 1px solid rgba(255, 255, 255, 0.05);
          border-radius: 12px;
          padding: 1.2rem;
          margin-bottom: 1rem;
          transition: all 0.3s ease;
        }
        .device-card:hover { border-color: var(--accent); box-shadow: 0 0 15px rgba(0, 242, 254, 0.1); }
        .device-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.8rem; }
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
      </style>
    </head>
    <body>
      <div class="container">
        <header>
          <div>
            <h1>Tuya Local Offline Integration</h1>
            <div class="logo-subtitle">Active Ingress Dashboard • Dark Aesthetics</div>
          </div>
        </header>

        <div class="layout">
          <div class="panel">
            <div class="panel-title">Synced LAN Devices</div>
            <div id="devices-list">
              <div style="text-align: center; color: var(--text-muted); padding: 2rem;">Scanning network and loading devices...</div>
            </div>
          </div>
        </div>
      </div>

      <script>
        // Load devices list and poll
        function refreshDevices() {
          fetch("api/devices")
            .then(r => r.json())
            .then(devices => {
              const container = document.getElementById("devices-list");
              if (devices.length === 0) {
                container.innerHTML = '<div style="text-align: center; color: var(--text-muted); padding: 2rem;">No devices loaded. Sync with Tuya Cloud first!</div>';
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
                      <div class="toggle-btn \\\${activeClass}" onclick="toggleRelay('\\ \${d.id}', \\\${key}, \\\${!isActive})">
                        <span>Relay \\\${key}</span>
                        <div class="indicator"></div>
                      </div>
                    \`;
                  }
                });

                if (!hasToggles) {
                  togglesHtml = '<div style="font-size: 0.85rem; color: var(--text-muted);">No active switch relays detected yet. Connecting...</div>';
                }

                html += \`
                  <div class="device-card">
                    <div class="device-header">
                      <div class="device-name">\\\${d.name}</div>
                      <span class="\\\${badgeClass}">\\\${statusText}</span>
                    </div>
                    <div class="device-info">
                      <strong>IP:</strong> \\\${d.ip} &nbsp;|&nbsp;
                      <strong>Device ID:</strong> \\\${d.id} &nbsp;|&nbsp;
                      <strong>Key:</strong> \\\${d.local_key}
                    </div>
                    <div class="entity-toggles">
                      \\\${togglesHtml}
                    </div>
                  </div>
                \`;
              });

              container.innerHTML = html;
            })
            .catch(err => console.error("Error refreshing devices:", err));
        }

        function toggleRelay(deviceId, channel, value) {
          // Clean dynamic string escaping issues if any
          const cleanId = deviceId.trim().replace(/^\\\s+/, "");
          fetch("api/control", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ deviceId: cleanId, channel, value })
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
  if (syncedDevices.length > 0) {
    localIpMappings = await getDeviceIps(syncedDevices);
    connectMqtt();
    
    syncedDevices.forEach(device => {
      const mapping = localIpMappings[device.id];
      if (mapping) {
        setupDevice(device, mapping);
      } else {
        console.warn(`[Main] Local IP mapping failed for ${device.name} (${device.id})`);
      }
    });
  } else {
    console.log("[Main] Cloud sync returned 0 devices. Web UI is active. Please configure credentials.");
  }
}

runLoops();
