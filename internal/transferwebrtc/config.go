package transferwebrtc

import (
	"github.com/pion/webrtc/v4"
)

// DefaultPeerConnectionConfig returns a WebRTC configuration with the given ICE servers.
func DefaultPeerConnectionConfig(stunServers, turnServers []string) webrtc.Configuration {
	var iceServers []webrtc.ICEServer

	// Add STUN servers
	if len(stunServers) > 0 {
		iceServers = append(iceServers, webrtc.ICEServer{
			URLs: stunServers,
		})
	}

	// Add TURN servers (each may have credentials)
	for _, turn := range turnServers {
		iceServers = append(iceServers, webrtc.ICEServer{
			URLs: []string{turn},
		})
	}

	return webrtc.Configuration{
		ICEServers: iceServers,
	}
}

// DefaultSettingEngine returns a SettingEngine configured for high throughput.
func DefaultSettingEngine() webrtc.SettingEngine {
	se := webrtc.SettingEngine{}

	// Enable detached data channels for direct io.ReadWriteCloser access
	se.DetachDataChannels()

	// Increase SCTP buffers for high throughput
	// 4MB receive buffer (default is usually small, e.g. 1MB or less)
	se.SetSCTPMaxReceiveBufferSize(4 * 1024 * 1024)

	return se
}

// NewPeerConnection creates a new PeerConnection with default settings.
func NewPeerConnection(config webrtc.Configuration) (*webrtc.PeerConnection, error) {
	se := DefaultSettingEngine()

	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	return api.NewPeerConnection(config)
}
