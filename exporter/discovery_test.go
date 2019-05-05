package exporter

import (
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestLoadRedisHosts(t *testing.T) {

	// todo: moar tests

	hosts, err := LoadRedisHosts("", "", "", ",", false)
	if err != nil {
		t.Errorf("Shouldn't have returned an error")
	}

	log.Printf("hosts: %#v", hosts)

	if len(hosts) != 1 {
		t.Errorf("Expected one host")
	}

	if hosts[0].Addr != "redis://localhost:6379" {
		t.Errorf("Wrong addr, got: %s", hosts[0].Addr)
	}
}

func TestLoadCommandLineArgs(t *testing.T) {
	tests := []struct {
		addr, pwd, sep      string
		wantAddrs, wantPwds []string
	}{
		{
			addr:      "",
			sep:       ",",
			wantAddrs: []string{"redis://localhost:6379"},
			wantPwds:  []string{""},
		},
		{
			addr:      "redis://localhost:6379",
			sep:       ",",
			wantAddrs: []string{"redis://localhost:6379"},
			wantPwds:  []string{""},
		},
		{
			addr:      "redis://localhost:6379,redis://localhost:7000",
			sep:       ",",
			wantAddrs: []string{"redis://localhost:6379", "redis://localhost:7000"},
			wantPwds:  []string{"", ""},
		},
		{
			addr:      "redis://localhost:6379,redis://localhost:7000,redis://localhost:7001",
			sep:       ",",
			wantAddrs: []string{"redis://localhost:6379", "redis://localhost:7000", "redis://localhost:7001"},
			wantPwds:  []string{"", "", ""},
		},
		{
			sep:       ",",
			wantAddrs: []string{"redis://localhost:6379"},
			wantPwds:  []string{""},
		},
	}

	for _, test := range tests {
		sep := test.sep
		hosts := LoadCommandLineArgs(test.addr, test.pwd, sep)
		checkHosts(t, hosts, test.wantAddrs, test.wantPwds)
	}
}

func TestLoadRedisFile(t *testing.T) {
	if _, err := LoadRedisFile("doesnt-exist.txt"); err == nil {
		t.Errorf("should have failed opening non existing file")
		return
	}

	hosts, err := LoadRedisFile("../contrib/sample_redis_hosts_file.txt")
	if err != nil {
		t.Errorf("LoadRedisFile() failed, err: %s", err)
		return
	}

	log.Debugf("hosts: %v \n", hosts)

	checkHosts(
		t, hosts,
		[]string{"redis://localhost:6379", "redis://localhost:7000", "redis://localhost:7000"},
		[]string{"", "password", "second-pwd"},
	)
}

func checkHosts(t *testing.T, hosts []RedisHost, addrs, pwds []string) {
	for _, addr := range addrs {
		found := false
		for _, host := range hosts {
			if host.Addr == addr {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Didn't find addr: %s, got hosts: %#v", addr, hosts)
			return
		}
	}

	for _, pwd := range pwds {
		found := false
		for _, host := range hosts {
			if host.Password == pwd {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Didn't find pwd: %s, got hosts: %#v", pwd, hosts)
			return
		}
	}
}

func TestGetCloudFoundryRedisBindings(t *testing.T) {
	GetCloudFoundryRedisBindings()
}
