// Package toolruntime implements the framework-owned, non-replaceable tool
// execution pipeline. It is internal so applications can extend policy without
// replacing panic recovery, cancellation, depth, deadlock or concurrency guards.
package toolruntime
