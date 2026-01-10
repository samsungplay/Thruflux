package app

import (
	"fmt"

	pionice "github.com/pion/ice/v2"
	"github.com/sheerbytes/sheerbytes/internal/ice"
)

func logICEPair(role, peerID string, peer *ice.ICEPeer) {
	if peer == nil {
		return
	}
	pair := peer.SelectedCandidatePair()
	if pair == nil {
		return
	}
	fmt.Printf("route %s peer=%s local=%s remote=%s\n", role, peerID, formatCandidate(pair.Local), formatCandidate(pair.Remote))
}

func formatCandidate(c pionice.Candidate) string {
	if c == nil {
		return ""
	}
	network := c.NetworkType().String()
	candType := c.Type().String()
	return fmt.Sprintf("%s/%s/%s:%d", candType, network, c.Address(), c.Port())
}
