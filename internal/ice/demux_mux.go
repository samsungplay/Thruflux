package ice

import (
	"fmt"
	"net"

	pionice "github.com/pion/ice/v2"
)

type demuxUDPMux struct {
	conns       map[pionice.NetworkType]*packetDemux
	listenAddrs []net.Addr
}

func newDemuxUDPMux(conns map[pionice.NetworkType]*packetDemux) (*demuxUDPMux, error) {
	addrs := make([]net.Addr, 0)
	for netType, demux := range conns {
		localAddrs, err := listenAddrsForConn(demux.conn, netType)
		if err != nil {
			return nil, err
		}
		addrs = append(addrs, localAddrs...)
	}
	return &demuxUDPMux{
		conns:       conns,
		listenAddrs: addrs,
	}, nil
}

func (m *demuxUDPMux) GetConn(ufrag string, addr net.Addr) (net.PacketConn, error) {
	if len(m.conns) == 0 {
		return nil, fmt.Errorf("no UDP conns available")
	}
	netType := networkTypeForAddr(addr)
	if demux, ok := m.conns[netType]; ok {
		return demux.STUNConn(), nil
	}
	for _, demux := range m.conns {
		return demux.STUNConn(), nil
	}
	return nil, fmt.Errorf("no UDP conn for address %v", addr)
}

func (m *demuxUDPMux) RemoveConnByUfrag(ufrag string) {
}

func (m *demuxUDPMux) GetListenAddresses() []net.Addr {
	out := make([]net.Addr, len(m.listenAddrs))
	copy(out, m.listenAddrs)
	return out
}

func (m *demuxUDPMux) Close() error {
	return nil
}

func networkTypeForAddr(addr net.Addr) pionice.NetworkType {
	if udpAddr, ok := addr.(*net.UDPAddr); ok && udpAddr.IP.To4() == nil {
		return pionice.NetworkTypeUDP6
	}
	return pionice.NetworkTypeUDP4
}

func listenAddrsForConn(conn *net.UDPConn, netType pionice.NetworkType) ([]net.Addr, error) {
	addr := conn.LocalAddr()
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return []net.Addr{addr}, nil
	}
	if !udpAddr.IP.IsUnspecified() {
		return []net.Addr{udpAddr}, nil
	}

	ips, err := localInterfaceIPs(netType, true)
	if err != nil {
		return nil, err
	}
	addrs := make([]net.Addr, 0, len(ips))
	for _, ip := range ips {
		addrs = append(addrs, &net.UDPAddr{IP: ip, Port: udpAddr.Port})
	}
	return addrs, nil
}

func localInterfaceIPs(netType pionice.NetworkType, includeLoopback bool) ([]net.IP, error) {
	ips := []net.IP{}
	ifaces, err := net.Interfaces()
	if err != nil {
		return ips, err
	}

	ipv4Requested := netType.IsIPv4()
	ipv6Requested := netType.IsIPv6()

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if !includeLoopback && iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch addr := addr.(type) {
			case *net.IPNet:
				ip = addr.IP
			case *net.IPAddr:
				ip = addr.IP
			}
			if ip == nil || (ip.IsLoopback() && !includeLoopback) {
				continue
			}

			if ipv4 := ip.To4(); ipv4 == nil {
				if !ipv6Requested {
					continue
				} else if !isSupportedIPv6(ip) {
					continue
				}
			} else if !ipv4Requested {
				continue
			}

			ips = append(ips, ip)
		}
	}
	return ips, nil
}

func isSupportedIPv6(ip net.IP) bool {
	if len(ip) != net.IPv6len ||
		isZeros(ip[0:12]) ||
		(ip[0] == 0xfe && ip[1]&0xc0 == 0xc0) ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() {
		return false
	}
	return true
}

func isZeros(ip net.IP) bool {
	for i := 0; i < len(ip); i++ {
		if ip[i] != 0 {
			return false
		}
	}
	return true
}
