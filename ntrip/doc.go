// Package ntrip provides NTRIP subscriber and source connections, an embedded
// caster, GGA generation, and mount-based real-time data fan-out.
//
// # Roles
//
// NTRIP uses client/server terminology from the perspective of the data flow:
//
//   - [NTRIPClient] connects to an external caster and subscribes to a mount.
//   - [NTRIPServer] connects to an external caster and publishes source data.
//   - [NTRIPCasterServer] accepts source connections for an embedded caster.
//   - [NTRIPCasterClient] accepts subscriber connections for an embedded caster.
//
// The two embedded-caster sides are connected with
// [NtripCasterServer.SetNtripCasterClient]. The subscriber side supports NTRIP
// v1 and v2 sourcetables through GET / and can optionally require an active
// source before accepting a subscription.
//
// # Connecting to a caster
//
// Use Connect when the caller needs to wait for the authentication result:
//
//	client := NewNTRIPClient("caster.example.com", 2101, "MOUNT", "user", "password")
//	client.OnData(func(key, mount string, data []byte, extra string) {
//		// data belongs to this callback and may be retained.
//	})
//	if err := client.Connect(ctx); err != nil {
//		return err
//	}
//	defer client.Shutdown(context.Background())
//
// Start retains the legacy asynchronous authentication behavior. Connect is
// generally preferable in new code because it reports dial and authentication
// failures directly. Stop initiates shutdown; Shutdown also waits for package
// goroutines to exit or for its context to expire.
//
// Connection configuration, including credentials, TLS settings, GGA fields,
// and retry delays, should be set before Start or Connect. To change it while a
// client is active, first call Shutdown, update the fields, and reconnect.
//
// # Embedded caster security
//
// Constructors bind the embedded caster to loopback by default. A caster bound
// to a non-loopback address refuses to start unless both source and subscriber
// authentication callbacks are explicitly configured. Production deployments
// should also configure TLS, total connection limits, and per-IP connection
// limits through [NTRIPCasterConfig] or the corresponding setters.
//
// Sourcetable access is public by default. Use
// [NtripCasterClient.SetRequireSourcetableAuth] to protect it; the authentication
// callback receives an empty mount for a sourcetable request. Use
// [NtripCasterClient.SetRequireActiveSource] to reject subscriptions whose mount
// has no active source.
//
// # Callbacks and data ownership
//
// Caster callbacks may run concurrently for different connections and should
// therefore be concurrency-safe and return promptly. Payload slices passed to
// public data callbacks are independent copies and may be retained after the
// callback returns.
//
// SetLogger replaces the package logger. Passing nil disables library logging.
//
// # Compatibility
//
// Canonical names use the NTRIP initialism, such as [NTRIPClient] and
// [NewNTRIPClient]. The older Ntrip-prefixed constructors and the DisConnect and
// NetError callback setters remain available for backward compatibility.
package ntrip
