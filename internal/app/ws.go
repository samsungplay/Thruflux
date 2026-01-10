package app

import (
	"fmt"
	"net/url"
	"strings"
)

func buildWebSocketURL(serverURL, joinCode, peerID, role string) (string, error) {
	u, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}

	scheme := strings.Replace(u.Scheme, "http", "ws", 1)
	if scheme == "ws" && u.Scheme == "https" {
		scheme = "wss"
	}

	wsURL := url.URL{
		Scheme:   scheme,
		Host:     u.Host,
		Path:     "/ws",
		RawQuery: fmt.Sprintf("join_code=%s&peer_id=%s&role=%s", url.QueryEscape(joinCode), url.QueryEscape(peerID), url.QueryEscape(role)),
	}

	return wsURL.String(), nil
}
