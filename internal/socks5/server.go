package socks5

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// Handler proxies SOCKS5 CONNECT requests through a tunnel opener.
type Handler struct {
	Open func(ctx context.Context, host string, port uint16) (net.Conn, error)
}

// Serve accepts SOCKS5 connections on ln until ctx is done.
func (h *Handler) Serve(ctx context.Context, ln net.Listener) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		go func(c net.Conn) {
			defer c.Close()
			_ = h.handle(ctx, c)
		}(conn)
	}
}

func (h *Handler) handle(ctx context.Context, conn net.Conn) error {
	if err := handshake(conn); err != nil {
		return err
	}
	host, port, err := readConnect(conn)
	if err != nil {
		return err
	}

	remote, err := h.Open(ctx, host, port)
	if err != nil {
		_ = writeReply(conn, 0x05)
		return err
	}
	defer remote.Close()

	if err := writeReply(conn, 0x00); err != nil {
		return err
	}

	go func() { _, _ = io.Copy(remote, conn) }()
	_, err = io.Copy(conn, remote)
	return err
}

func handshake(conn net.Conn) error {
	buf := make([]byte, 2)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return err
	}
	if buf[0] != 0x05 {
		return fmt.Errorf("unsupported SOCKS version %d", buf[0])
	}
	methods := make([]byte, buf[1])
	if _, err := io.ReadFull(conn, methods); err != nil {
		return err
	}
	_, err := conn.Write([]byte{0x05, 0x00})
	return err
}

func readConnect(conn net.Conn) (string, uint16, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return "", 0, err
	}
	if hdr[0] != 0x05 || hdr[1] != 0x01 {
		return "", 0, fmt.Errorf("unsupported command %d", hdr[1])
	}

	var host string
	switch hdr[3] {
	case 0x01:
		ip := make([]byte, 4)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return "", 0, err
		}
		host = net.IP(ip).String()
	case 0x03:
		var ln [1]byte
		if _, err := io.ReadFull(conn, ln[:]); err != nil {
			return "", 0, err
		}
		name := make([]byte, ln[0])
		if _, err := io.ReadFull(conn, name); err != nil {
			return "", 0, err
		}
		host = string(name)
	case 0x04:
		ip := make([]byte, 16)
		if _, err := io.ReadFull(conn, ip); err != nil {
			return "", 0, err
		}
		host = net.IP(ip).String()
	default:
		return "", 0, fmt.Errorf("unsupported address type %d", hdr[3])
	}

	var portBuf [2]byte
	if _, err := io.ReadFull(conn, portBuf[:]); err != nil {
		return "", 0, err
	}
	port := binary.BigEndian.Uint16(portBuf[:])
	return host, port, nil
}

func writeReply(conn net.Conn, code byte) error {
	// VER REP RSV ATYP BND.ADDR BND.PORT
	_, err := conn.Write([]byte{0x05, code, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	return err
}
