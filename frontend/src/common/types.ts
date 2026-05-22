export interface StartReceivePayload {
  joinCode: string;
  out: string;
  serverUrl: string;
  stunServer: string;
  turnServers: string;
  forceTurn: boolean;
  quicConnWindowBytes: number;
  quicStreamWindowBytes: number;
  overwrite: boolean;
  udpBufferBytes: number;
}

export interface StartHostPayload {
  paths: string[];
  serverUrl: string;
  maxReceivers: number;
  stunServer: string;
  turnServers: string;
  forceTurn: boolean;
  quicStreamWindowBytes: number;
  quicConnWindowBytes: number;
  udpBufferBytes: number;
  "custom-join-code"?: string;
}

export interface AbortReceiverPayload {
  receiverId: string;
}

export interface ApiErrorBody {
  error: string;
}

export interface ConnectingEvent {
  type: "connecting";
  message: "";
}

export interface ConnectErrorEvent {
  type: "connect_error";
  message: {
    code: number;
    reason: string;
  };
}

export interface ConnectSuccessEvent {
  type: "connect_success";
  message: "";
}

export interface DisconnectedEvent {
  type: "disconnected";
  message: {
    reason: string;
  };
}

export interface ProgressEvent {
  type: "progress";
  message: {
    receiverId: string;
    ewmaThroughput: number;
    bytesMoved: number;
    skippedBytes: number;
    filesMoved: number;
    totalExpectedFilesCount: number;
    isRelayed: boolean;
    percent: number;
    hasError: boolean;
  };
}

export interface JoinCodeIssuedEvent {
  type: "join_code_issued";
  message: {
    join_code: string;
  };
}

export interface ManifestBuildStartEvent {
  type: "manifest_build_start";
  message: "";
}

export interface ManifestBuildProgressEvent {
  type: "manifest_build_progress";
  message: {
    files_count: number;
    total_size: number;
  };
}

export interface ManifestEncodingEvent {
  type: "manifest_encoding";
  message: "";
}

export interface ManifestSealedEvent {
  type: "manifest_sealed";
  message: {
    files_count: number;
    total_size: number;
  };
}

export interface P2pFailedEvent {
  type: "p2p_failed";
  message: "";
}

export interface JoiningSessionEvent {
  type: "joining_session";
  message: "";
}

export interface P2pStartEvent {
  type: "p2p_start";
  message: "";
}

export interface P2pSuccessEvent {
  type: "p2p_success";
  message: "";
}

export interface ManifestReceiveProgressEvent {
  type: "manifest_receive_progress";
  message: {
    total_size: number;
    complete: boolean;
  };
}

export interface ManifestReceiveErrorEvent {
  type: "manifest_receive_error";
  message: {
    errno: string | number;
  };
}

export interface ManifestParsingEvent {
  type: "manifest_parsing";
  message: "";
}

export interface ResumeNoticeEvent {
  type: "resume_notice";
  message: {
    percent: number;
  };
}

export interface ManifestUnsealedEvent {
  type: "manifest_unsealed";
  message: {
    files_count: number;
    total_size: number;
  };
}

export interface QuicHandshakeSuccessEvent {
  type: "quic_handshake_success";
  message: "";
}

export interface IceNotReadyEvent {
  type: "ice_not_ready";
  message: "";
}

export interface ReceiveCompleteEvent {
  type: "receive_complete";
  message: "";
}

export type ThrufluxEvent =
  | ConnectingEvent
  | ConnectErrorEvent
  | ConnectSuccessEvent
  | DisconnectedEvent
  | ProgressEvent
  | JoinCodeIssuedEvent
  | ManifestBuildStartEvent
  | ManifestBuildProgressEvent
  | ManifestEncodingEvent
  | ManifestSealedEvent
  | P2pFailedEvent
  | JoiningSessionEvent
  | P2pStartEvent
  | P2pSuccessEvent
  | ManifestReceiveProgressEvent
  | ManifestReceiveErrorEvent
  | ManifestParsingEvent
  | ResumeNoticeEvent
  | ManifestUnsealedEvent
  | QuicHandshakeSuccessEvent
  | IceNotReadyEvent
  | ReceiveCompleteEvent;

export interface UnknownEvent {
  type: string;
  message: unknown;
}

export type ParsedThrufluxEvent = ThrufluxEvent | UnknownEvent;

export interface EngineUnavailableError {
  code: "ENGINE_UNAVAILABLE";
  message: string;
}

export interface EndpointError {
  code: "ENDPOINT_ERROR";
  status: number;
  message: string;
  body?: unknown;
}

export type ThrufluxClientError = EngineUnavailableError | EndpointError;

export interface HealthResult {
  alive: boolean;
  baseUrl: string | null;
  port: number | null;
}

export interface CommandResult {
  status: number;
  ok: boolean;
  body: unknown;
}

export type Unsubscribe = () => void;
