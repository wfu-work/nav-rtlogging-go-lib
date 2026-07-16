package ntrip

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

type ntripRequest struct {
	method  string
	target  string
	parts   []string
	headers map[string]string
}

func findNtripRequestHeaderEnd(data []byte) int {
	if end := bytes.Index(data, []byte("\r\n\r\n")); end >= 0 {
		return end + 4
	}
	if end := bytes.Index(data, []byte("\n\n")); end >= 0 {
		return end + 2
	}
	return -1
}

func parseNtripRequest(data []byte) (ntripRequest, error) {
	normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	if len(lines) == 0 {
		return ntripRequest{}, errors.New("empty ntrip request")
	}
	parts := strings.Fields(lines[0])
	if len(parts) < 2 {
		return ntripRequest{}, fmt.Errorf("invalid ntrip request line %q", lines[0])
	}
	request := ntripRequest{
		method:  strings.ToUpper(parts[0]),
		parts:   parts,
		headers: make(map[string]string),
	}
	switch request.method {
	case "SOURCE":
		if len(parts) < 3 {
			return ntripRequest{}, fmt.Errorf("invalid SOURCE request line %q", lines[0])
		}
		request.target = normalizeMount(parts[2])
	default:
		request.target = normalizeMount(parts[1])
	}
	if request.target == "" {
		return ntripRequest{}, errors.New("empty ntrip mount point")
	}
	for _, line := range lines[1:] {
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		request.headers[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
	}
	return request, nil
}

func normalizeMount(target string) string {
	target = strings.TrimSpace(target)
	return strings.Trim(target, "/")
}

func parseBasicAuthorization(value string) (string, string, error) {
	scheme, encoded, ok := strings.Cut(strings.TrimSpace(value), " ")
	if !ok || !strings.EqualFold(scheme, "Basic") || strings.TrimSpace(encoded) == "" {
		return "", "", errors.New("missing Basic authorization credentials")
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return "", "", fmt.Errorf("decode Basic authorization: %w", err)
	}
	username, password, ok := strings.Cut(string(decoded), ":")
	if !ok {
		return "", "", errors.New("invalid Basic authorization credentials")
	}
	return username, password, nil
}

func ntripAuthResponse(authorized bool) string {
	if !authorized {
		return "HTTP/1.0 401 Unauthorized\r\nConnection: close\r\n\r\n"
	}
	return "ICY 200 OK\r\nServer: nav-rtlogging-go-lib\r\nDate: " + NowNtripDate() + "\r\n\r\n"
}

func ntripAuthResponseForRequest(authorized bool, request ntripRequest) string {
	isV2 := len(request.parts) >= 3 && strings.EqualFold(request.parts[2], "HTTP/1.1")
	if !isV2 {
		return ntripAuthResponse(authorized)
	}
	if !authorized {
		return "HTTP/1.1 401 Unauthorized\r\nNtrip-Version: Ntrip/2.0\r\nConnection: close\r\n\r\n"
	}
	return "HTTP/1.1 200 OK\r\nNtrip-Version: Ntrip/2.0\r\nServer: nav-rtlogging-go-lib\r\nDate: " + NowNtripDate() + "\r\n\r\n"
}
