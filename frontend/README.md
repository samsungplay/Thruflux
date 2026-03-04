# Thruflux Frontend

Electron desktop frontend for Thruflux.

Build and run scripts are cross-platform (macOS, Linux, Windows).

## Prerequisites

- Node.js 18+ (recommended)
- npm

## Install dependencies

```bash
npm install
```

## Run locally

```bash
npm run start
```

This builds the app and starts Electron.

## Build

```bash
npm run build
```

Build output:

- `dist/main/main.js`
- `dist/renderer/renderer.js`
- `dist/renderer/index.html`

## Build installers

```bash
npm run dist
```

Installer output directory: `dist/`

Configured installer artifact names:

- macOS: `Thruflux-macos-arm64.pkg`
- Windows: `Thruflux-windows-amd64.exe`
- Linux: `Thruflux-linux-amd64.deb`
