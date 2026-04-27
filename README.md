# NXMSG Server Release

This folder is the GitHub-ready server package for Render deploys.

## Included files

- `server.js`
- `package.json`
- `package-lock.json`
- `.env.example`
- `render.yaml`
- `public/`

## Deployment target

- Platform: Render web service
- Realtime: WebSocket enabled by default
- Fallback API: HTTP endpoints remain available
- Calls: supported with WebSocket signaling

## Keepalive

The server includes an internal keepalive loop for free Render instances.

- Default interval: 5 minutes
- Default target: `RENDER_EXTERNAL_URL`
- Optional override: `NXMSG_KEEPALIVE_TARGET_URL`

Important: if you keep a free Render service awake all month, it will consume almost all included free instance hours.

## Required environment variables

- `DATABASE_URL`

Optional:

- `FIREBASE_SERVICE_ACCOUNT_JSON`
- `FIREBASE_SERVICE_ACCOUNT_JSON_BASE64`
- `NXMSG_STUN_URL`
- `NXMSG_TURN_URL`
- `NXMSG_TURN_USERNAME`
- `NXMSG_TURN_PASSWORD`

See `RENDER_SETUP.md` for the deployment steps.
