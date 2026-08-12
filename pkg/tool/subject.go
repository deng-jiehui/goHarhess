package tool

import (
	"context"
	"errors"
)

// ErrSubjectNotFound means a permission policy required an authenticated Subject.
var ErrSubjectNotFound = errors.New("tool authorization subject not found in context")

// Role is an application-defined authorization role.
type Role string

// Permission is an application-defined capability identifier.
type Permission string

// Subject is the authenticated identity used by tool authorization policies.
type Subject struct {
	ID          string
	TenantID    string
	Roles       []Role
	Permissions []Permission
}

// HasRole reports whether the Subject owns role.
func (s Subject) HasRole(role Role) bool {
	for _, current := range s.Roles {
		if current == role {
			return true
		}
	}
	return false
}

// HasPermission reports whether the Subject owns permission.
func (s Subject) HasPermission(permission Permission) bool {
	for _, current := range s.Permissions {
		if current == permission {
			return true
		}
	}
	return false
}

type subjectContextKey struct{}

// WithSubject returns a child context containing a defensive copy of subject.
// As with context.WithValue, ctx must not be nil.
func WithSubject(ctx context.Context, subject Subject) context.Context {
	return context.WithValue(ctx, subjectContextKey{}, cloneSubject(subject))
}

// SubjectFromContext returns a defensive copy of the current authorization Subject.
func SubjectFromContext(ctx context.Context) (Subject, bool) {
	if ctx == nil {
		return Subject{}, false
	}
	subject, ok := ctx.Value(subjectContextKey{}).(Subject)
	if !ok {
		return Subject{}, false
	}
	return cloneSubject(subject), true
}

// RequireSubject returns the current Subject or ErrSubjectNotFound.
func RequireSubject(ctx context.Context) (Subject, error) {
	subject, ok := SubjectFromContext(ctx)
	if !ok {
		return Subject{}, ErrSubjectNotFound
	}
	return subject, nil
}

func cloneSubject(subject Subject) Subject {
	subject.Roles = append([]Role(nil), subject.Roles...)
	subject.Permissions = append([]Permission(nil), subject.Permissions...)
	return subject
}
