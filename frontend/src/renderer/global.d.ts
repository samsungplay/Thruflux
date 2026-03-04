import type { ThrufluxBridge } from '../common/ipc'

declare global {
  interface Window {
    thruflux: ThrufluxBridge
  }
}

declare module "*.css"

export {}
