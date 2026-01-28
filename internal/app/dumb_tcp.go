package app

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"
)

func dumbTCPListenAddrs(port int) []string {
	addrs := make([]string, 0)
	ifaces, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range ifaces {
			ipnet, ok := addr.(*net.IPNet)
			if !ok || ipnet.IP == nil {
				continue
			}
			if ipnet.IP.IsLoopback() {
				continue
			}
			if ip4 := ipnet.IP.To4(); ip4 != nil {
				addrs = append(addrs, net.JoinHostPort(ip4.String(), strconv.Itoa(port)))
			}
		}
	}
	if len(addrs) == 0 {
		addrs = append(addrs, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	}
	return addrs
}

func acceptWithContext(ctx context.Context, ln net.Listener) (net.Conn, error) {
	type res struct {
		conn net.Conn
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		ch <- res{conn: c, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = ln.Close()
		return nil, ctx.Err()
	case r := <-ch:
		return r.conn, r.err
	}
}

func dialAddrs(ctx context.Context, addrs []string) (net.Conn, error) {
	d := net.Dialer{Timeout: 5 * time.Second}
	var lastErr error
	for _, addr := range addrs {
		conn, err := d.DialContext(ctx, "tcp", addr)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no addresses to dial")
	}
	return nil, lastErr
}
