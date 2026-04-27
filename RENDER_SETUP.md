# Render Setup

## 1. Push this folder to GitHub

Use `nxmsg-server-release` as the root of the server repository.

## 2. Create the Render web service

You can deploy either:

- from the included `render.yaml`
- or by creating a Node web service manually

Recommended settings:

- Runtime: Node
- Build command: `npm install`
- Start command: `npm start`
- Health check path: `/health`

## 3. Add environment variables

Required:

- `DATABASE_URL`

Optional but recommended:

- `FIREBASE_SERVICE_ACCOUNT_JSON`
- `FIREBASE_SERVICE_ACCOUNT_JSON_BASE64`
- `NXMSG_RENDER_KEEPALIVE_ENABLED=true`
- `NXMSG_STUN_URL=stun:stun.l.google.com:19302`
- `NXMSG_TURN_URL`
- `NXMSG_TURN_USERNAME`
- `NXMSG_TURN_PASSWORD`

## 4. Android values

After the first deploy, copy the live host into Android:

```properties
NXMSG_API_BASE=https://your-app.onrender.com
NXMSG_WS_BASE=wss://your-app.onrender.com
NXMSG_TEST_MODE=false
```

## 5. Expected result

- register and login
- chat list and message history
- realtime messaging over WebSocket
- file sending
- incoming and outgoing calls
- push notifications when Firebase is configured
