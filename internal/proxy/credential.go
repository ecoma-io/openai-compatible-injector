package proxy

import (
	"openai-compatible-injector/internal/credential"
)

// CredentialResolver is the handler's seam to the process's credential
// pools — transport.Resolver one layer up. The handler resolves a
// credential-bearing candidate's pool once per candidate, before the
// candidate's first attempt, and holds the returned *Pool for the
// candidate's whole walk, so rotation and cooldown state is never rebuilt
// mid-request even across a reload. A nil resolver — or a candidate whose
// configuration carries no auth block — disables the seam entirely: such
// requests run the historical no-credential path byte for byte.
//
// The resolver hands out rotation STATE, never credential material to the
// wire machinery: the pool returns a whole Key at Acquire and the header
// value is composed in the handler alone, onto the request the transport
// is already about to send. Nothing credential-shaped reaches the
// transport layer — pools live beside the transports, never inside them.
type CredentialResolver interface {
	Pool(*credential.Provider) *credential.Pool
}
