// server.js
// Minimal relay server: serves a controller webpage + routes WS messages controller<->robot.

const express = require("express");
const http = require("http");
const path = require("path");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;

// Simple shared secrets (rotate later; move to proper auth when ready)
const ROBOT_TOKEN = process.env.ROBOT_TOKEN || "robot_secret";
const USER_TOKEN  = process.env.USER_TOKEN  || "user_secret";

const app = express();
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

/**
 * Connection registries
 * robotSockets: robotId -> ws
 * controllerSockets: controllerId -> ws
 * controllerWants: controllerId -> robotId
 */
const robotSockets = new Map();
const controllerSockets = new Map();
const controllerWants = new Map();

function safeSend(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function closeWith(ws, code, reason) {
  try { ws.close(code, reason); } catch {}
}

wss.on("connection", (ws, req) => {
  ws.isAuthed = false;
  ws.role = null;
  ws.id = null;

  // Expect hello within 5s
  const helloTimeout = setTimeout(() => {
    if (!ws.isAuthed) closeWith(ws, 4001, "hello timeout");
  }, 5000);

  ws.on("message", (buf) => {
    let msg;
    try {
      msg = JSON.parse(buf.toString("utf8"));
    } catch {
      return safeSend(ws, { type: "error", error: "invalid_json" });
    }

    // First message must be hello
    if (!ws.isAuthed) {
      if (msg.type !== "hello") {
        return safeSend(ws, { type: "error", error: "expected_hello" });
      }

      const { role, token } = msg;
      if (role === "robot") {
        const { robotId } = msg;
        if (!robotId) return safeSend(ws, { type: "error", error: "missing_robotId" });
        if (token !== ROBOT_TOKEN) return closeWith(ws, 4003, "bad token");

        ws.isAuthed = true;
        ws.role = "robot";
        ws.id = robotId;

        // Replace previous connection if exists
        const old = robotSockets.get(robotId);
        if (old && old !== ws) closeWith(old, 4000, "replaced");
        robotSockets.set(robotId, ws);

        clearTimeout(helloTimeout);
        safeSend(ws, { type: "hello_ok", role: "robot", robotId });

        // Notify any controllers waiting for this robot
        for (const [controllerId, wantsRobot] of controllerWants.entries()) {
          if (wantsRobot === robotId) {
            const cws = controllerSockets.get(controllerId);
            safeSend(cws, { type: "robot_status", robotId, online: true });
          }
        }
        return;
      }

      if (role === "controller") {
        const { controllerId, wantsRobot } = msg;
        if (!controllerId) return safeSend(ws, { type: "error", error: "missing_controllerId" });
        if (!wantsRobot) return safeSend(ws, { type: "error", error: "missing_wantsRobot" });
        if (token !== USER_TOKEN) return closeWith(ws, 4003, "bad token");

        ws.isAuthed = true;
        ws.role = "controller";
        ws.id = controllerId;

        const old = controllerSockets.get(controllerId);
        if (old && old !== ws) closeWith(old, 4000, "replaced");
        controllerSockets.set(controllerId, ws);
        controllerWants.set(controllerId, wantsRobot);

        clearTimeout(helloTimeout);
        safeSend(ws, { type: "hello_ok", role: "controller", controllerId, wantsRobot });

        const rws = robotSockets.get(wantsRobot);
        safeSend(ws, { type: "robot_status", robotId: wantsRobot, online: !!rws });
        return;
      }

      return safeSend(ws, { type: "error", error: "bad_role" });
    }

    // After auth: route messages
    if (ws.role === "controller") {
      const wantsRobot = controllerWants.get(ws.id);
      const rws = robotSockets.get(wantsRobot);

      if (!rws) {
        return safeSend(ws, { type: "robot_status", robotId: wantsRobot, online: false });
      }

      // Only forward allowed message types
      if (msg.type === "drive" || msg.type === "stop" || msg.type === "ping") {
        safeSend(rws, { ...msg, from: ws.id });
      } else {
        safeSend(ws, { type: "error", error: "unsupported_message_type" });
      }
      return;
    }

    if (ws.role === "robot") {
      // Forward telemetry/status back to all controllers that want this robot
      if (msg.type === "telemetry" || msg.type === "status" || msg.type === "pong") {
        for (const [controllerId, wantsRobot] of controllerWants.entries()) {
          if (wantsRobot === ws.id) {
            const cws = controllerSockets.get(controllerId);
            safeSend(cws, { ...msg, robotId: ws.id });
          }
        }
      } else {
        safeSend(ws, { type: "error", error: "unsupported_message_type" });
      }
      return;
    }
  });

  ws.on("close", () => {
    clearTimeout(helloTimeout);

    if (ws.role === "robot" && ws.id) {
      // Only delete if we are still the registered one
      if (robotSockets.get(ws.id) === ws) robotSockets.delete(ws.id);

      // Notify controllers
      for (const [controllerId, wantsRobot] of controllerWants.entries()) {
        if (wantsRobot === ws.id) {
          const cws = controllerSockets.get(controllerId);
          safeSend(cws, { type: "robot_status", robotId: ws.id, online: false });
        }
      }
    }

    if (ws.role === "controller" && ws.id) {
      if (controllerSockets.get(ws.id) === ws) controllerSockets.delete(ws.id);
      controllerWants.delete(ws.id);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Relay server listening on http://0.0.0.0:${PORT}`);
  console.log(`Serve controller UI from /public`);
});
