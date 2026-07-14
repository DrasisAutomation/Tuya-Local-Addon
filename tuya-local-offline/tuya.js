const crypto = require("crypto");

class TuyaClient {
  constructor({ clientId, secret, baseUrl }) {
    this.clientId = clientId;
    this.secret = secret;
    this.baseUrl = baseUrl || "https://openapi.tuyain.com";
    this.accessToken = null;
    this.tokenExpireTime = 0;
  }

  calculateSignature(method, path, body = "", accessToken = null) {
    const t = Date.now().toString();
    const nonce = "";

    const contentSha256 = crypto
      .createHash("sha256")
      .update(body)
      .digest("hex");

    const stringToSign = `${method}\n${contentSha256}\n\n${path}`;

    const str = accessToken
      ? `${this.clientId}${accessToken}${t}${nonce}${stringToSign}`
      : `${this.clientId}${t}${nonce}${stringToSign}`;

    const sign = crypto
      .createHmac("sha256", this.secret)
      .update(str)
      .digest("hex")
      .toUpperCase();

    return { sign, t };
  }

  async getAccessToken() {
    if (this.accessToken && Date.now() < this.tokenExpireTime) {
      return this.accessToken;
    }

    const path = "/v1.0/token?grant_type=1";
    const method = "GET";
    const { sign, t } = this.calculateSignature(method, path, "");

    const headers = {
      client_id: this.clientId,
      sign: sign,
      t: t,
      sign_method: "HMAC-SHA256",
    };

    try {
      const response = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers,
      });

      const data = await response.json();
      if (!data.success) {
        throw new Error(`Tuya Authentication Failed: ${data.msg} (Code: ${data.code})`);
      }

      this.accessToken = data.result.access_token;
      this.tokenExpireTime = Date.now() + (data.result.expire_time - 60) * 1000;
      
      return this.accessToken;
    } catch (error) {
      console.error("[TuyaClient] Failed to get access token:", error);
      throw error;
    }
  }

  async request({ method, path, body = "" }) {
    const token = await this.getAccessToken();
    const { sign, t } = this.calculateSignature(method, path, body, token);

    const headers = {
      client_id: this.clientId,
      access_token: token,
      sign: sign,
      t: t,
      sign_method: "HMAC-SHA256",
    };

    if (body) {
      headers["Content-Type"] = "application/json";
    }

    try {
      const response = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers,
        body: body ? body : undefined,
      });

      return await response.json();
    } catch (error) {
      console.error(`[TuyaClient] Request error on ${path}:`, error);
      throw error;
    }
  }
}

module.exports = { TuyaClient };
