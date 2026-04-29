package collector

import (
	"testing"
	"time"
)

func TestParseGetentName(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "passwd",
			raw:  "alice:x:1000:1000:Alice Example:/home/alice:/bin/bash\n",
			want: "alice",
		},
		{
			name: "group",
			raw:  "project:x:1028:alice,bob\n",
			want: "project",
		},
		{
			name: "empty",
			raw:  "",
			want: "",
		},
		{
			name: "invalid",
			raw:  "not-a-getent-record\n",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseGetentName(tt.raw); got != tt.want {
				t.Fatalf("parseGetentName(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestUnixIDResolverUsesCachedMiss(t *testing.T) {
	resolver := newUnixIDResolver()
	resolver.users["12345"] = cachedIDName{name: "", expires: time.Now().Add(time.Minute)}

	if got := resolver.ResolveUser("12345"); got != "" {
		t.Fatalf("ResolveUser returned %q, want cached miss", got)
	}
}
