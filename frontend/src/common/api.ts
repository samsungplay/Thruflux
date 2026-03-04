import type {
  AbortReceiverPayload,
  ApiErrorBody,
  CommandResult,
  EndpointError,
  EngineUnavailableError,
  HealthResult,
  ParsedThrufluxEvent,
  StartHostPayload,
  StartReceivePayload,
  Unsubscribe,
} from "./types";

const DEFAULT_HOST = "127.0.0.1";
const DEFAULT_TIMEOUT_MS = 1500;
const PORTS: number[] = [
  48480, 48481, 48482, 48483, 48484, 48485, 48486, 48487,
];

let cachedBaseUrl: string | null = null;
let preferredBaseUrl: string | null = null;

export class ThrufluxApiError extends Error {
  readonly code: EngineUnavailableError["code"] | EndpointError["code"];
  readonly status?: number;
  readonly body?: unknown;

  constructor(params: {
    code: EngineUnavailableError["code"] | EndpointError["code"];
    message: string;
    status?: number;
    body?: unknown;
  }) {
    super(params.message);
    this.name = "ThrufluxApiError";
    this.code = params.code;
    this.status = params.status;
    this.body = params.body;
  }
}

function baseUrlForPort(port: number, host = DEFAULT_HOST): string {
  return `http://${host}:${port}`;
}

function tryParsePort(baseUrl: string): number | null {
  try {
    const parsed = new URL(baseUrl);
    const portValue = Number.parseInt(parsed.port, 10);
    return Number.isFinite(portValue) ? portValue : null;
  } catch {
    return null;
  }
}

async function fetchWithTimeout(
  input: string,
  init: RequestInit = {},
  timeoutMs = DEFAULT_TIMEOUT_MS,
): Promise<Response> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(input, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timeout);
  }
}

async function parseResponseBody(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) {
    return null;
  }
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function parseEventPayload(payload: string): ParsedThrufluxEvent {
  try {
    return JSON.parse(payload) as ParsedThrufluxEvent;
  } catch {
    return { type: "invalid_event", message: payload };
  }
}

function extractEndpointErrorMessage(status: number, body: unknown): string {
  if (body && typeof body === "object" && "error" in body) {
    const candidate = (body as ApiErrorBody).error;
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate;
    }
  }
  return `Request failed with status ${status}`;
}

async function tryHealth(baseUrl: string): Promise<boolean> {
  try {
    const response = await fetchWithTimeout(`${baseUrl}/health`, {
      method: "GET",
    });
    return response.ok;
  } catch {
    return false;
  }
}

async function resolveBaseUrl(forceScan = false): Promise<string> {
  if (preferredBaseUrl && (await tryHealth(preferredBaseUrl))) {
    cachedBaseUrl = preferredBaseUrl;
    return preferredBaseUrl;
  }

  if (!forceScan && cachedBaseUrl) {
    return cachedBaseUrl;
  }

  for (const port of PORTS) {
    const baseUrl = baseUrlForPort(port);
    if (await tryHealth(baseUrl)) {
      cachedBaseUrl = baseUrl;
      return baseUrl;
    }
  }

  cachedBaseUrl = null;
  throw new ThrufluxApiError({
    code: "ENGINE_UNAVAILABLE",
    message:
      "No Thruflux engine found on localhost. Please try restarting the app.",
  });
}

export function setPreferredBaseUrl(baseUrl: string | null): void {
  preferredBaseUrl = baseUrl;
  if (baseUrl) {
    cachedBaseUrl = baseUrl;
  }
}

async function request(
  path: string,
  init: RequestInit = {},
): Promise<CommandResult> {
  let baseUrl: string;
  try {
    baseUrl = await resolveBaseUrl(false);
  } catch {
    baseUrl = await resolveBaseUrl(true);
  }

  let response: Response;
  try {
    response = await fetchWithTimeout(`${baseUrl}${path}`, init);
  } catch {
    cachedBaseUrl = null;
    baseUrl = await resolveBaseUrl(true);
    response = await fetchWithTimeout(`${baseUrl}${path}`, init);
  }

  const body = await parseResponseBody(response);

  if (!response.ok) {
    throw new ThrufluxApiError({
      code: "ENDPOINT_ERROR",
      status: response.status,
      body,
      message: extractEndpointErrorMessage(response.status, body),
    });
  }

  return {
    status: response.status,
    ok: response.ok,
    body,
  };
}

export async function health(): Promise<HealthResult> {
  if (preferredBaseUrl) {
    const alive = await tryHealth(preferredBaseUrl);
    if (alive) {
      cachedBaseUrl = preferredBaseUrl;
      return {
        alive: true,
        baseUrl: preferredBaseUrl,
        port: tryParsePort(preferredBaseUrl),
      };
    }
  }

  for (const port of PORTS) {
    const baseUrl = baseUrlForPort(port);
    if (await tryHealth(baseUrl)) {
      cachedBaseUrl = baseUrl;
      return { alive: true, baseUrl, port };
    }
  }
  cachedBaseUrl = null;
  return { alive: false, baseUrl: null, port: null };
}

export async function startHost(
  payload: StartHostPayload,
): Promise<CommandResult> {
  return request("/host", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function startReceive(
  payload: StartReceivePayload,
): Promise<CommandResult> {
  return request("/receive", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function abort(): Promise<CommandResult> {
  return request("/abort", {
    method: "POST",
  });
}

export async function abortReceiver(
  payload: AbortReceiverPayload,
): Promise<CommandResult> {
  return request("/abortReceiver", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
}

export async function subscribeEvents(
  onEvent: (event: ParsedThrufluxEvent) => void,
): Promise<Unsubscribe> {
  const baseUrl = await resolveBaseUrl(false);
  const EventSourceCtor = globalThis.EventSource;

  if (!EventSourceCtor) {
    throw new ThrufluxApiError({
      code: "ENGINE_UNAVAILABLE",
      message: "EventSource is not available in this runtime",
    });
  }

  const source = new EventSourceCtor(`${baseUrl}/events`);

  source.onmessage = (evt: MessageEvent<string>) => {
    onEvent(parseEventPayload(evt.data));
  };

  source.onerror = () => {
    cachedBaseUrl = null;
  };

  return () => {
    source.close();
  };
}

export const thrufluxApi = {
  health,
  startHost,
  startReceive,
  abort,
  abortReceiver,
  subscribeEvents,
  setPreferredBaseUrl,
};
