package tool_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"goharness/pkg/tool"
)

func TestSubjectContextUsesTypedImmutableValue(t *testing.T) {
	subject := tool.Subject{
		ID:          "user-1",
		TenantID:    "tenant-a",
		Roles:       []tool.Role{"viewer"},
		Permissions: []tool.Permission{"documents:read"},
	}
	ctx := tool.WithSubject(context.Background(), subject)
	subject.Roles[0] = "admin"
	subject.Permissions[0] = "documents:delete"

	got, err := tool.RequireSubject(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "user-1" || got.TenantID != "tenant-a" {
		t.Fatalf("subject = %#v", got)
	}
	if !got.HasRole("viewer") || got.HasRole("admin") {
		t.Fatalf("roles = %#v", got.Roles)
	}
	if !got.HasPermission("documents:read") || got.HasPermission("documents:delete") {
		t.Fatalf("permissions = %#v", got.Permissions)
	}

	got.Roles[0] = "admin"
	again, ok := tool.SubjectFromContext(ctx)
	if !ok || !again.HasRole("viewer") {
		t.Fatalf("stored subject was mutated: %#v", again)
	}
}

func TestRequireSubjectReportsMissingContextIdentity(t *testing.T) {
	_, err := tool.RequireSubject(context.Background())
	if !errors.Is(err, tool.ErrSubjectNotFound) {
		t.Fatalf("RequireSubject error = %v", err)
	}
}

func ExampleWithSubject() {
	ctx := tool.WithSubject(context.Background(), tool.Subject{
		ID:          "user-1",
		Roles:       []tool.Role{"viewer"},
		Permissions: []tool.Permission{"documents:read"},
	})
	subject, err := tool.RequireSubject(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(subject.ID, subject.HasRole("viewer"), subject.HasPermission("documents:read"))
	// Output: user-1 true true
}
