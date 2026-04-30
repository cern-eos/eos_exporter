package collector

import (
	"context"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	idResolveSuccessTTL = 24 * time.Hour
	idResolveMissTTL    = 5 * time.Minute
	idResolveTimeout    = 2 * time.Second
)

type cachedIDName struct {
	name    string
	expires time.Time
}

type unixIDResolver struct {
	mu     sync.Mutex
	users  map[string]cachedIDName
	groups map[string]cachedIDName
}

func newUnixIDResolver() *unixIDResolver {
	return &unixIDResolver{
		users:  make(map[string]cachedIDName),
		groups: make(map[string]cachedIDName),
	}
}

func (r *unixIDResolver) ResolveUser(uid string) string {
	return r.resolve(uid, r.users, "passwd")
}

func (r *unixIDResolver) ResolveGroup(gid string) string {
	return r.resolve(gid, r.groups, "group")
}

func (r *unixIDResolver) resolve(id string, cache map[string]cachedIDName, database string) string {
	if id == "" {
		return ""
	}

	now := time.Now()

	r.mu.Lock()
	cached, ok := cache[id]
	if ok && now.Before(cached.expires) {
		r.mu.Unlock()
		if cached.name == "" {
			return id
		}
		return cached.name
	}
	r.mu.Unlock()

	name := lookupIDName(database, id)
	ttl := idResolveMissTTL
	if name != "" {
		ttl = idResolveSuccessTTL
	}

	r.mu.Lock()
	cache[id] = cachedIDName{name: name, expires: now.Add(ttl)}
	r.mu.Unlock()

	if name == "" {
		return id
	}

	return name
}

func lookupIDName(database, id string) string {
	ctx, cancel := context.WithTimeout(context.Background(), idResolveTimeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, "getent", database, id).Output()
	if err != nil {
		return ""
	}

	return parseGetentName(string(out))
}

func parseGetentName(raw string) string {
	line := strings.TrimSpace(raw)
	if line == "" {
		return ""
	}

	if idx := strings.IndexByte(line, ':'); idx > 0 {
		return line[:idx]
	}

	return ""
}
