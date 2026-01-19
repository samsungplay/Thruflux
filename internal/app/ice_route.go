package app

import (
	"fmt"
)

// logICEPair is deprecated but kept for compatibility.
func logICEPair(role, peerID, route string) {
	if route != "" {
		fmt.Printf("route %s peer=%s %s\n", role, peerID, route)
	}
}

// iceRouteString is deprecated.
func iceRouteString(role, peerID string, _ any) string {
	return ""
}
