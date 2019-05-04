package exporter

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/cloudfoundry-community/go-cfenv"
)

func LoadRedisHosts(redisAddr, redisPassword, redisFile, separator string, useCfBindings bool) ([]RedisHost, error) {
	if useCfBindings {
		return GetCloudFoundryRedisBindings()
	}

	if redisFile != "" && redisAddr != "" {
		return nil, fmt.Errorf("Cannot specify both redis.addr and redis.file")
	}

	if redisFile != "" {
		return LoadRedisFile(redisFile)
	}

	hosts := LoadCommandLineArgs(redisAddr, redisPassword, separator)
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no hosts found")
	}

	return hosts, nil
}

// loadRedisArgs loads the configuration for which redis hosts to monitor from either
// the environment or as passed from program arguments. Returns the list of host addrs,
// passwords.
func LoadCommandLineArgs(addr, password, separator string) []RedisHost {
	if addr == "" {
		addr = "redis://localhost:6379"
	}
	var res []RedisHost
	addrs := strings.Split(addr, separator)
	passwords := strings.Split(password, separator)
	for idx, addr := range addrs {
		var pwd string
		if idx < len(passwords) {
			pwd = passwords[idx]
		}
		res = append(res, RedisHost{Addr: addr, Password: pwd})
	}
	return res
}

// loadRedisFile opens the specified file and loads the configuration for which redis
// hosts to monitor. Returns the list of hosts addrs and passwords.
func LoadRedisFile(fileName string) ([]RedisHost, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(file)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	file.Close()
	// For each line, test if it contains an optional password and provide them,
	// else give them empty strings

	var res []RedisHost
	for _, record := range records {
		var addr, pwd string
		switch len(record) {
		case 2:
			addr = record[0]
			pwd = record[1]
		case 1:
			addr = record[0]

		default:
			continue
		}
		res = append(res, RedisHost{Addr: addr, Password: pwd})
	}
	return res, nil
}

func GetCloudFoundryRedisBindings() ([]RedisHost, error) {
	if !cfenv.IsRunningOnCF() {
		return nil, fmt.Errorf("IsRunningOnCF() returned false - not running on CF ?")
	}

	appEnv, err := cfenv.Current()
	if err != nil {
		return nil, fmt.Errorf("Unable to get current CF environment, err: %s", err)
	}

	redisServices, err := appEnv.Services.WithTag("redis")
	if err != nil {
		return nil, fmt.Errorf("Error while getting redis services, err: %s", err)
	}

	var res []RedisHost
	for _, redisService := range redisServices {
		credentials := redisService.Credentials
		host := getAlternative(credentials, "host", "hostname")
		port := getAlternative(credentials, "port")
		addr := host + ":" + port
		pwd := getAlternative(credentials, "pwd")
		res = append(res, RedisHost{Addr: addr, Password: pwd})
	}

	return res, nil
}

func getAlternative(credentials map[string]interface{}, alternatives ...string) string {
	for _, key := range alternatives {
		if value, ok := credentials[key]; ok {
			return value.(string)
		}
	}
	return ""
}
