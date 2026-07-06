package protocol

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

// ConnectRequest opens a proxied flow to a service or host.
type ConnectRequest struct {
	Type           string `json:"type"`
	TargetService  string `json:"target_service"`
	TargetHost     string `json:"target_host,omitempty"`
	TargetPort     uint32 `json:"target_port"`
	ClientIdentity string `json:"client_identity"`
}

// ConnectResponse is the server's reply to ConnectRequest.
type ConnectResponse struct {
	Type         string `json:"type"`
	OK           bool   `json:"ok"`
	Reason       string `json:"reason,omitempty"`
	ResolvedNode string `json:"resolved_node,omitempty"`
}

const (
	FrameConnect    = "connect"
	FrameConnectAck = "connect_ack"
)

func WriteConnectRequest(w io.Writer, req ConnectRequest) error {
	req.Type = FrameConnect
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}

func ReadConnectResponse(r *bufio.Reader) (ConnectResponse, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return ConnectResponse{}, err
	}
	var resp ConnectResponse
	if err := json.Unmarshal(line, &resp); err != nil {
		return ConnectResponse{}, err
	}
	return resp, nil
}

func WriteConnectResponse(w io.Writer, resp ConnectResponse) error {
	resp.Type = FrameConnectAck
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = w.Write(data)
	return err
}

func ReadConnectRequest(r *bufio.Reader) (ConnectRequest, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return ConnectRequest{}, err
	}
	var req ConnectRequest
	if err := json.Unmarshal(line, &req); err != nil {
		return ConnectRequest{}, err
	}
	if req.Type != FrameConnect {
		return ConnectRequest{}, fmt.Errorf("unexpected frame type %q", req.Type)
	}
	return req, nil
}

func Relay(a, b io.ReadWriteCloser) error {
	errCh := make(chan error, 2)
	go func() { errCh <- copyAndClose(a, b) }()
	go func() { errCh <- copyAndClose(b, a) }()
	err1 := <-errCh
	err2 := <-errCh
	if err1 != nil && err1 != io.EOF {
		return err1
	}
	if err2 != nil && err2 != io.EOF {
		return err2
	}
	return nil
}

func copyAndClose(dst io.WriteCloser, src io.ReadCloser) error {
	defer dst.Close()
	defer src.Close()
	_, err := io.Copy(dst, src)
	return err
}
