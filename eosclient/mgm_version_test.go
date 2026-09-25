package eosclient

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseLocalMGMRole(t *testing.T) {
	isLeader, leader, err := parseLocalMGMRole("uid=all gid=all is_master=true master_id=eospilot-ns-02.cern.ch:1094\n")
	if err != nil || !isLeader || leader != "eospilot-ns-02.cern.ch:1094" {
		t.Fatalf("isLeader=%t leader=%q, err=%v", isLeader, leader, err)
	}
	isLeader, leader, err = parseLocalMGMRole("uid=all gid=all is_master=false master_id=eospilot-ns-02.cern.ch:1094\n")
	if err != nil || isLeader || leader != "" {
		t.Fatalf("isLeader=%t leader=%q, err=%v", isLeader, leader, err)
	}
	for _, output := range []string{
		"uid=all gid=all master_id=eospilot-ns-02.cern.ch:1094",
		"uid=all gid=all is_master=true master_id=invalid",
		"uid=all gid=all is_master=maybe master_id=eospilot-ns-02.cern.ch:1094",
	} {
		if _, _, err := parseLocalMGMRole(output); err == nil {
			t.Fatalf("accepted invalid role output %q", output)
		}
	}
}

func TestParseMGMVersion(t *testing.T) {
	version, release, err := parseMGMVersion("EOS_INSTANCE=eospilot\nEOS_SERVER_VERSION=5.5.2 EOS_SERVER_RELEASE=1\nEOS_CLIENT_VERSION=5.4.3 EOS_CLIENT_RELEASE=1\n")
	if err != nil || version != "5.5.2" || release != "1" {
		t.Fatalf("version=%q release=%q err=%v", version, release, err)
	}
	if _, _, err := parseMGMVersion("EOS_CLIENT_VERSION=5.4.3 EOS_CLIENT_RELEASE=1"); err == nil {
		t.Fatal("accepted client version as MGM version")
	}
}

func TestGetLocalMGMLeaderVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "eos")
	script := "#!/bin/sh\n" +
		"case \"$1\" in\n" +
		"  ns) [ \"$EOS_MGM_URL\" = 'root://localhost' ] || exit 1; echo \"uid=all gid=all is_master=${TEST_ROLE:-true} master_id=local.example:1094\" ;;\n" +
		"  version) [ \"$EOS_MGM_URL\" = 'root://localhost' ] && [ \"$TEST_ROLE\" != false ] || exit 1; echo 'EOS_SERVER_VERSION=5.5.2 EOS_SERVER_RELEASE=1' ;;\n" +
		"  *) exit 1 ;;\n" +
		"esac\n"
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	client, err := New(&Options{EosBinary: path, URL: "root://localhost"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := client.GetLocalMGMLeaderVersion(context.Background())
	if err != nil || got == nil || got.Leader != "local.example:1094" || got.Version != "5.5.2" || got.Release != "1" {
		t.Fatalf("leader version=%+v, err=%v", got, err)
	}
	t.Setenv("TEST_ROLE", "false")
	got, err = client.GetLocalMGMLeaderVersion(context.Background())
	if err != nil || got != nil {
		t.Fatalf("follower reported a leader version=%+v, err=%v", got, err)
	}
}
