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

	// Enable detached data channels for better performance
	se.DetachDataChannels()

	return se
}

// NewPeerConnection creates a new PeerConnection with default settings.
func NewPeerConnection(config webrtc.Configuration) (*webrtc.PeerConnection, error) {
	se := DefaultSettingEngine()

	api := webrtc.NewAPI(webrtc.WithSettingEngine(se))
	return api.NewPeerConnection(config)
}
