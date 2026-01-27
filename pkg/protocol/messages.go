package protocol

// Hello is sent when a peer first connects to the server.
type Hello struct {
	PeerID       string         `json:"peer_id"`
	Role         string         `json:"role"`
	Capabilities map[string]any `json:"capabilities,omitempty"`
}

// Error represents an error message in the protocol.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// PeerInfo contains information about a peer.
type PeerInfo struct {
	PeerID string `json:"peer_id"`
	Role   string `json:"role"`
}

// PeerList contains a list of peers.
type PeerList struct {
	Peers []PeerInfo `json:"peers"`
}

// PeerJoined indicates a peer has joined a session.
type PeerJoined struct {
	Peer PeerInfo `json:"peer"`
}

// PeerLeft indicates a peer has left a session.
type PeerLeft struct {
	PeerID string `json:"peer_id"`
}

// CreateSessionRequest requests creation of a new session.
type CreateSessionRequest struct {
	Role string `json:"role,omitempty"`
}

// CreateSessionResponse responds with session details.
type CreateSessionResponse struct {
	SessionID string `json:"session_id"`
	JoinCode  string `json:"join_code"`
}

// JoinSession requests to join an existing session.
type JoinSession struct {
	JoinCode string `json:"join_code"`
	PeerID   string `json:"peer_id"`
	Role     string `json:"role"`
}

// Offer contains an offer for P2P connection establishment (opaque SDP string).
type Offer struct {
	SDP string `json:"sdp"`
}

// Answer contains an answer for P2P connection establishment (opaque SDP string).
type Answer struct {
	SDP string `json:"sdp"`
}

// IceCandidate contains an ICE candidate for P2P connection establishment (opaque candidate string).
type IceCandidate struct {
	Candidate string `json:"candidate"`
}

// IceCredentials contains ICE credentials (username fragment and password) for P2P connection establishment.
type IceCredentials struct {
	Ufrag string `json:"ufrag"`
	Pwd   string `json:"pwd"`
}

// IceCandidates contains all gathered ICE candidates.
type IceCandidates struct {
	Candidates []string `json:"candidates"`
}

// TurnCredentials contains TURN server URLs with embedded ephemeral credentials.
type TurnCredentials struct {
	Servers   []string `json:"servers"`
	ExpiresAt string   `json:"expires_at,omitempty"`
}

// ManifestSummary contains summary information about a file manifest.
type ManifestSummary struct {
	ManifestID  string `json:"manifest_id"`
	TotalBytes  int64  `json:"total_bytes"`
	FileCount   int    `json:"file_count"`
	FolderCount int    `json:"folder_count"`
	RootName    string `json:"root_name"`
}

// ManifestOffer offers a manifest for transfer.
type ManifestOffer struct {
	Summary ManifestSummary `json:"summary"`
}

// ManifestAccept accepts a manifest offer.
type ManifestAccept struct {
	ManifestID    string   `json:"manifest_id"`
	Mode          string   `json:"mode"`
	SelectedPaths []string `json:"selected_paths,omitempty"`
}

// TransferStart signals a receiver to begin ICE/QUIC for a snapshot transfer.
type TransferStart struct {
	ManifestID     string `json:"manifest_id"`
	SenderPeerID   string `json:"sender_peer_id"`
	ReceiverPeerID string `json:"receiver_peer_id"`
	TransferID     string `json:"transfer_id"`
}

// TransferQueued indicates a receiver is queued for a transfer slot.
type TransferQueued struct {
	ManifestID     string `json:"manifest_id"`
	ReceiverPeerID string `json:"receiver_peer_id"`
	Position       int    `json:"position"`
	Active         int    `json:"active"`
	Max            int    `json:"max"`
}
