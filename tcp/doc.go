// Package tcp provides context-aware TCP client and server wrappers with
// lifecycle management, callbacks, connection limits, and complete-write
// handling.
//
// # Client lifecycle
//
// [TcpClient.Connect] establishes a connection and uses its context to cancel
// an in-progress dial. Start is a convenience wrapper around Connect with a
// background context. Stop closes the current connection, while Shutdown also
// waits for the read goroutine to exit or for its context to expire.
//
//	client := NewTCPClient("127.0.0.1", 2101)
//	client.OnData(func(conn net.Conn, data []byte) {
//		// data belongs to this callback and may be retained.
//	})
//	if err := client.Connect(ctx); err != nil {
//		return err
//	}
//	defer client.Shutdown(context.Background())
//
// Write serializes concurrent calls and continues until all bytes have been
// written or an error occurs.
//
// # Server lifecycle and limits
//
// [TCPServer] accepts connections until Stop or Shutdown is called. Configure
// total and per-IP connection limits with [Server.SetMaxConnections] and
// [Server.SetMaxConnectionsPerIP] before starting the server. A zero per-IP
// limit disables that limit; the default total limit is 1024 connections.
//
// Server callbacks may run concurrently for different connections and should
// be concurrency-safe and return promptly. Payload slices passed to OnData are
// independent copies and may be retained after the callback returns.
//
// # Compatibility
//
// New code should prefer [TCPClient], [TCPServer], [NewTCPClient],
// [NewTCPServer], OnDisconnect, and OnError. The older TcpClient, NewTcps,
// DisConnect, and NetError names remain available for backward compatibility.
package tcp
