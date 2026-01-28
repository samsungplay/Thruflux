package protocol

// Message type constants for protocol envelopes.
const (
	TypeHello                 = "hello"
	TypeError                 = "error"
	TypePeerList              = "peer_list"
	TypePeerJoined            = "peer_joined"
	TypePeerLeft              = "peer_left"
	TypeCreateSessionRequest  = "create_session_request"
	TypeCreateSessionResponse = "create_session_response"
	TypeJoinSession           = "join_session"
	TypeOffer                 = "offer"
	TypeAnswer                = "answer"
	TypeIceCandidate          = "ice_candidate"
	TypeIceCredentials        = "ice_credentials"
	TypeIceCandidates         = "ice_candidates"
	TypeTurnCredentials       = "turn_credentials"
	TypeManifestOffer         = "manifest_offer"
	TypeManifestAccept        = "manifest_accept"
	TypeTransferStart         = "transfer_start"
	TypeTransferQueued        = "transfer_queued"
	TypeDumbTCPListen         = "dumb_tcp_listen"
	TypeDumbQUICMulti         = "dumb_quic_multi"
)
