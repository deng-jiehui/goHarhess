// Package callback provides synchronous, Context-scoped observation plumbing.
// Domain lifecycle interfaces live with their components.
package callback

// Handler supplies a stable diagnostic identity. Domain packages extend this
// marker with strongly typed lifecycle methods.
type Handler interface {
	Name() string
}
