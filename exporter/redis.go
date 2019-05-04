package exporter

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prom_strutil "github.com/prometheus/prometheus/util/strutil"
	log "github.com/sirupsen/logrus"
)

type RedisHost struct {
	Addr     string
	Password string
}

type dbKeyPair struct {
	db, key string
}

type keyInfo struct {
	size    float64
	keyType string
}

// Exporter implements the prometheus.Exporter interface, and exports Redis metrics.
type Exporter struct {
	sync.Mutex
	hosts      []RedisHost
	namespace  string
	keys       []dbKeyPair
	singleKeys []dbKeyPair

	totalScrapes              prometheus.Counter
	targetScrapeDuration      prometheus.Summary
	targetScrapeRequestErrors prometheus.Counter

	metricDescriptions map[string]*prometheus.Desc

	options   Options
	LuaScript []byte
}

type Options struct {
	Namespace              string
	ConfigCommandName      string
	CheckSingleKeys        string
	CheckKeys              string
	IncludeVerbotenMetrics bool
	SkipTLSVerification    bool
}

var (
	metricMapGauges = map[string]string{
		// # Server
		"uptime_in_seconds": "uptime_in_seconds",
		"process_id":        "process_id",

		// # Clients
		"connected_clients":          "connected_clients",
		"client_longest_output_list": "client_longest_output_list",
		"client_biggest_input_buf":   "client_biggest_input_buf",
		"blocked_clients":            "blocked_clients",

		// # Memory
		"allocator_active":    "allocator_active_bytes",
		"allocator_allocated": "allocator_allocated_bytes",
		"allocator_resident":  "allocator_resident_bytes",
		"used_memory":         "memory_used_bytes",
		"used_memory_rss":     "memory_used_rss_bytes",
		"used_memory_peak":    "memory_used_peak_bytes",
		"used_memory_lua":     "memory_used_lua_bytes",
		"maxmemory":           "memory_max_bytes",

		// # Persistence
		"rdb_changes_since_last_save":  "rdb_changes_since_last_save",
		"rdb_bgsave_in_progress":       "rdb_bgsave_in_progress",
		"rdb_last_save_time":           "rdb_last_save_timestamp_seconds",
		"rdb_last_bgsave_status":       "rdb_last_bgsave_status",
		"rdb_last_bgsave_time_sec":     "rdb_last_bgsave_duration_sec",
		"rdb_current_bgsave_time_sec":  "rdb_current_bgsave_duration_sec",
		"rdb_last_cow_size":            "rdb_last_cow_size_bytes",
		"aof_enabled":                  "aof_enabled",
		"aof_rewrite_in_progress":      "aof_rewrite_in_progress",
		"aof_rewrite_scheduled":        "aof_rewrite_scheduled",
		"aof_last_rewrite_time_sec":    "aof_last_rewrite_duration_sec",
		"aof_current_rewrite_time_sec": "aof_current_rewrite_duration_sec",
		"aof_last_cow_size":            "aof_last_cow_size_bytes",
		"aof_current_size":             "aof_current_size_bytes",
		"aof_base_size":                "aof_base_size_bytes",
		"aof_pending_rewrite":          "aof_pending_rewrite",
		"aof_buffer_length":            "aof_buffer_length",
		"aof_rewrite_buffer_length":    "aof_rewrite_buffer_length",
		"aof_pending_bio_fsync":        "aof_pending_bio_fsync",
		"aof_delayed_fsync":            "aof_delayed_fsync",
		"aof_last_bgrewrite_status":    "aof_last_bgrewrite_status",
		"aof_last_write_status":        "aof_last_write_status",

		// # Stats
		"instantaneous_ops_per_sec": "instantaneous_ops_per_sec",
		"instantaneous_input_kbps":  "instantaneous_input_kbps",
		"instantaneous_output_kbps": "instantaneous_output_kbps",
		"pubsub_channels":           "pubsub_channels",
		"pubsub_patterns":           "pubsub_patterns",
		"latest_fork_usec":          "latest_fork_usec",

		// # Replication
		"loading":                    "loading_dump_file",
		"connected_slaves":           "connected_slaves",
		"repl_backlog_size":          "replication_backlog_bytes",
		"master_last_io_seconds_ago": "master_last_io_seconds",
		"master_repl_offset":         "master_repl_offset",

		// # Cluster
		"cluster_stats_messages_sent":     "cluster_messages_sent_total",
		"cluster_stats_messages_received": "cluster_messages_received_total",

		// # Tile38
		// based on https://tile38.com/commands/server/
		"aof_size":        "aof_size_bytes",
		"avg_item_size":   "avg_item_size_bytes",
		"cpus":            "cpus_total",
		"heap_released":   "heap_released_bytes",
		"heap_size":       "heap_size_bytes",
		"http_transport":  "http_transport",
		"in_memory_size":  "in_memory_size_bytes",
		"max_heap_size":   "max_heap_size_bytes",
		"mem_alloc":       "mem_alloc_bytes",
		"num_collections": "num_collections_total",
		"num_hooks":       "num_hooks_total",
		"num_objects":     "num_objects_total",
		"num_points":      "num_points_total",
		"pointer_size":    "pointer_size_bytes",
		"read_only":       "read_only",
		"threads":         "threads_total",
		"version":         "version", // since tile38 version 1.14.1
	}

	metricMapCounters = map[string]string{
		"total_connections_received": "connections_received_total",
		"total_commands_processed":   "commands_processed_total",

		"rejected_connections":   "rejected_connections_total",
		"total_net_input_bytes":  "net_input_bytes_total",
		"total_net_output_bytes": "net_output_bytes_total",

		"expired_keys":    "expired_keys_total",
		"evicted_keys":    "evicted_keys_total",
		"keyspace_hits":   "keyspace_hits_total",
		"keyspace_misses": "keyspace_misses_total",

		"used_cpu_sys":           "used_cpu_sys_seconds_total",
		"used_cpu_user":          "used_cpu_user_seconds_total",
		"used_cpu_sys_children":  "used_cpu_sys_children_seconds_total",
		"used_cpu_user_children": "used_cpu_user_children_seconds_total",
	}

	instanceInfoFields = map[string]bool{"role": true, "redis_version": true, "redis_build_id": true, "redis_mode": true, "os": true}
	slaveInfoFields    = map[string]bool{"master_host": true, "master_port": true, "slave_read_only": true}
)

func (e *Exporter) ScrapeHandler(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("target")
	if target == "" {
		http.Error(w, "'target' parameter must be specified", 400)
		e.targetScrapeRequestErrors.Inc()
		return
	}

	start := time.Now()
	exp, _ := NewRedisExporter([]RedisHost{{Addr: target}}, e.options)
	registry := prometheus.NewRegistry()
	registry.MustRegister(exp)
	h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})

	h.ServeHTTP(w, r)

	duration := time.Since(start).Seconds()
	e.targetScrapeDuration.Observe(duration)
	log.Debugf("Scrape of target '%s' took %f seconds", target, duration)
}

// splitKeyArgs splits a command-line supplied argument into a slice of dbKeyPairs.
func parseKeyArg(keysArgString string) (keys []dbKeyPair, err error) {
	if keysArgString == "" {
		return keys, err
	}
	for _, k := range strings.Split(keysArgString, ",") {
		db := "0"
		key := ""
		frags := strings.Split(k, "=")
		switch len(frags) {
		case 1:
			db = "0"
			key, err = url.QueryUnescape(strings.TrimSpace(frags[0]))
		case 2:
			db = strings.Replace(strings.TrimSpace(frags[0]), "db", "", -1)
			key, err = url.QueryUnescape(strings.TrimSpace(frags[1]))
		default:
			return keys, fmt.Errorf("invalid key list argument: %s", k)
		}
		if err != nil {
			return keys, fmt.Errorf("couldn't parse db/key string: %s", k)
		}

		keys = append(keys, dbKeyPair{db, key})
	}
	return keys, err
}

func newMetricDescr(namespace string, metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(namespace, "", metricName), docString, labels, nil)
}

// NewRedisExporter returns a new exporter of Redis metrics.
func NewRedisExporter(hosts []RedisHost, opts Options) (*Exporter, error) {
	e := Exporter{
		hosts:     hosts,
		options:   opts,
		namespace: opts.Namespace,

		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: opts.Namespace,
			Name:      "exporter_scrapes_total",
			Help:      "Current total redis scrapes.",
		}),

		targetScrapeDuration: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Name: "target_scrape_collection_duration_seconds",
				Help: "Duration of collections by the SNMP exporter",
			},
		),
		targetScrapeRequestErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "target_scrape_request_errors_total",
				Help: "Errors in requests to the SNMP exporter",
			},
		),
	}

	if e.options.ConfigCommandName == "" {
		e.options.ConfigCommandName = "CONFIG"
	}

	var err error

	if e.keys, err = parseKeyArg(opts.CheckKeys); err != nil {
		return &e, fmt.Errorf("Couldn't parse check-keys: %#v", err)
	}
	log.Debugf("keys: %#v", e.keys)

	if e.singleKeys, err = parseKeyArg(opts.CheckSingleKeys); err != nil {
		return &e, fmt.Errorf("Couldn't parse check-single-keys: %#v", err)
	}
	log.Debugf("singleKeys: %#v", e.singleKeys)

	if opts.IncludeVerbotenMetrics {
		metricMapGauges["total_system_memory"] = "total_system_memory_bytes"
	}

	l := []string{}
	e.metricDescriptions = map[string]*prometheus.Desc{}
	e.metricDescriptions["up"] = newMetricDescr(opts.Namespace, "up", "Information about the Redis instance", l)
	e.metricDescriptions["instance_info"] = newMetricDescr(opts.Namespace, "instance_info", "Information about the Redis instance", []string{"role", "redis_version", "redis_build_id", "redis_mode", "os"})
	e.metricDescriptions["last_scrape_duration"] = newMetricDescr(opts.Namespace, "exporter_last_scrape_duration_seconds", "The last scrape duration", l)
	e.metricDescriptions["scrape_error"] = newMetricDescr(opts.Namespace, "exporter_last_scrape_error", "The last scrape error status.", []string{"err"})

	e.metricDescriptions["script_values"] = newMetricDescr(opts.Namespace, "script_value", "Values returned by the collect script", []string{"key"})
	e.metricDescriptions["key_values"] = newMetricDescr(opts.Namespace, "key_value", `The value of "key"`, []string{"db", "key"})
	e.metricDescriptions["key_sizes"] = newMetricDescr(opts.Namespace, "key_size", `The length or size of "key"`, []string{"db", "key"})

	e.metricDescriptions["commands_total"] = newMetricDescr(opts.Namespace, "commands_total", `Total number of calls per command`, []string{"cmd"})
	e.metricDescriptions["commands_duration_seconds_total"] = newMetricDescr(opts.Namespace, "commands_duration_seconds_total", `Total amount of time in seconds spent per command`, []string{"cmd"})
	e.metricDescriptions["slowlog_length"] = newMetricDescr(opts.Namespace, "slowlog_length", `Total slowlog`, l)
	e.metricDescriptions["slowlog_last_id"] = newMetricDescr(opts.Namespace, "slowlog_last_id", `Last id of slowlog`, l)
	e.metricDescriptions["last_slow_execution_duration_seconds"] = newMetricDescr(opts.Namespace, "last_slow_execution_duration_seconds", `The amount of time needed for last slow execution, in seconds`, l)

	e.metricDescriptions["latency_spike_last"] = newMetricDescr(opts.Namespace, "latency_spike_last", `When the latency spike last occurred`, []string{"event_name"})
	e.metricDescriptions["latency_spike_seconds"] = newMetricDescr(opts.Namespace, "latency_spike_seconds", `Length of the last latency spike in seconds`, []string{"event_name"})

	e.metricDescriptions["slave_info"] = newMetricDescr(opts.Namespace, "slave_info", "Information about the Redis slave", []string{"master_host", "master_port", "read_only"})

	e.metricDescriptions["start_time_seconds"] = newMetricDescr(opts.Namespace, "start_time_seconds", "Start time of the Redis instance since unix epoch in seconds.", l)
	e.metricDescriptions["master_link_up"] = newMetricDescr(opts.Namespace, "master_link_up", "Master link status on Redis slave", l)
	e.metricDescriptions["connected_slave_offset"] = newMetricDescr(opts.Namespace, "connected_slave_offset", "Offset of connected slave", []string{"slave_ip", "slave_port", "slave_state"})
	e.metricDescriptions["connected_slave_lag_seconds"] = newMetricDescr(opts.Namespace, "connected_slave_lag_seconds", "Lag of connected slave", []string{"slave_ip", "slave_port", "slave_state"})

	e.metricDescriptions["db_keys"] = newMetricDescr(opts.Namespace, "db_keys", "Total number of keys by DB", []string{"db"})
	e.metricDescriptions["db_keys_expiring"] = newMetricDescr(opts.Namespace, "db_keys_expiring", "Total number of expiring keys by DB", []string{"db"})
	e.metricDescriptions["db_avg_ttl_seconds"] = newMetricDescr(opts.Namespace, "db_avg_ttl_seconds", "Avg TTL in seconds", []string{"db"})

	return &e, nil
}

// Describe outputs Redis metric descriptions.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range e.metricDescriptions {
		ch <- desc
	}

	for _, v := range metricMapGauges {
		ch <- newMetricDescr(e.options.Namespace, v, v+" metric", []string{})
	}

	for _, v := range metricMapCounters {
		ch <- newMetricDescr(e.options.Namespace, v, v+" metric", []string{})
	}

	ch <- e.totalScrapes.Desc()
}

// Collect fetches new metrics from the RedisHost and updates the appropriate metrics.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.Lock()
	defer e.Unlock()

	e.scrapeAllHosts(ch)

	ch <- e.totalScrapes
}

func includeMetric(s string) bool {
	if strings.HasPrefix(s, "db") || strings.HasPrefix(s, "cmdstat_") || strings.HasPrefix(s, "cluster_") {
		return true
	}
	if _, ok := metricMapGauges[s]; ok {
		return true
	}

	_, ok := metricMapCounters[s]
	return ok
}

func sanitizeMetricName(n string) string {
	return prom_strutil.SanitizeLabelName(n)
}

func extractVal(s string) (val float64, err error) {
	split := strings.Split(s, "=")
	if len(split) != 2 {
		return 0, fmt.Errorf("nope")
	}
	val, err = strconv.ParseFloat(split[1], 64)
	if err != nil {
		return 0, fmt.Errorf("nope")
	}
	return
}

/*
	valid example: db0:keys=1,expires=0,avg_ttl=0
*/
func parseDBKeyspaceString(db string, stats string) (keysTotal float64, keysExpiringTotal float64, avgTTL float64, ok bool) {
	ok = false
	if !strings.HasPrefix(db, "db") {
		return
	}

	split := strings.Split(stats, ",")
	if len(split) != 3 && len(split) != 2 {
		return
	}

	var err error
	ok = true
	if keysTotal, err = extractVal(split[0]); err != nil {
		ok = false
		return
	}
	if keysExpiringTotal, err = extractVal(split[1]); err != nil {
		ok = false
		return
	}

	avgTTL = -1
	if len(split) > 2 {
		if avgTTL, err = extractVal(split[2]); err != nil {
			ok = false
			return
		}
		avgTTL /= 1000
	}
	return
}

/*
	slave0:ip=10.254.11.1,port=6379,state=online,offset=1751844676,lag=0
	slave1:ip=10.254.11.2,port=6379,state=online,offset=1751844222,lag=0
*/
func parseConnectedSlaveString(slaveName string, slaveInfo string) (offset float64, ip string, port string, state string, lag float64, ok bool) {
	ok = false
	if matched, _ := regexp.MatchString(`^slave\d+`, slaveName); !matched {
		return
	}
	connectedSlaveInfo := make(map[string]string)
	for _, kvPart := range strings.Split(slaveInfo, ",") {
		x := strings.Split(kvPart, "=")
		if len(x) != 2 {
			log.Debugf("Invalid format for connected slave string, got: %s", kvPart)
			return
		}
		connectedSlaveInfo[x[0]] = x[1]
	}
	offset, err := strconv.ParseFloat(connectedSlaveInfo["offset"], 64)
	if err != nil {
		log.Debugf("Can not parse connected slave offset, got: %s", connectedSlaveInfo["offset"])
		return
	}

	if lagStr, exists := connectedSlaveInfo["lag"]; exists == false {
		// Prior to 3.0, "lag" property does not exist
		lag = -1
	} else {
		lag, err = strconv.ParseFloat(lagStr, 64)
		if err != nil {
			log.Debugf("Can not parse connected slave lag, got: %s", lagStr)
			return
		}
	}

	ok = true
	ip = connectedSlaveInfo["ip"]
	port = connectedSlaveInfo["port"]
	state = connectedSlaveInfo["state"]

	return
}

func (e *Exporter) extractConfigMetrics(ch chan<- prometheus.Metric, config []string, host RedisHost) (dbCount int, err error) {
	if len(config)%2 != 0 {
		return 0, fmt.Errorf("invalid config: %#v", config)
	}

	for pos := 0; pos < len(config)/2; pos++ {
		strKey := config[pos*2]
		strVal := config[pos*2+1]

		if strKey == "databases" {
			if dbCount, err = strconv.Atoi(strVal); err != nil {
				return 0, fmt.Errorf("invalid config value for key databases: %#v", strVal)
			}
		}

		// todo: we can add more configs to this map if there's interest
		if !map[string]bool{
			"maxmemory":  true,
			"maxclients": true,
		}[strKey] {
			continue
		}

		if val, err := strconv.ParseFloat(strVal, 64); err == nil {
			// todo: this needs a test
			e.registerGaugeValue(ch, fmt.Sprintf("config_%s", config[pos*2]), val, []string{})
		}
	}
	return
}

func (e *Exporter) registerGaugeValue(ch chan<- prometheus.Metric, metric string, val float64, labels []string) {
	e.registerMetricValue(ch, metric, val, prometheus.GaugeValue, labels)
}

func (e *Exporter) registerMetricValue(ch chan<- prometheus.Metric, metric string, val float64, valType prometheus.ValueType, labels []string) {
	descr := e.metricDescriptions[metric]
	if descr == nil {
		descr = newMetricDescr(e.options.Namespace, metric, metric+" metric", []string{})
	}

	ch <- prometheus.MustNewConstMetric(descr, valType, val, labels...)
}

func (e *Exporter) extractTile38Metrics(ch chan<- prometheus.Metric, info []string, host RedisHost) error {
	for i := 0; i < len(info); i += 2 {
		log.Debugf("tile38: %s:%s", info[i], info[i+1])

		fieldKey := info[i]
		fieldValue := info[i+1]

		if !includeMetric(fieldKey) {
			continue
		}

		e.parseAndRegisterMetric(ch, host, fieldKey, fieldValue)
	}

	return nil
}

func (e *Exporter) handleMetricsCommandStats(ch chan<- prometheus.Metric, host RedisHost, fieldKey string, fieldValue string) {
	/*
		Format:
		cmdstat_get:calls=21,usec=175,usec_per_call=8.33
		cmdstat_set:calls=61,usec=3139,usec_per_call=51.46
		cmdstat_setex:calls=75,usec=1260,usec_per_call=16.80
	*/
	splitKey := strings.Split(fieldKey, "_")
	if len(splitKey) != 2 {
		return
	}

	splitValue := strings.Split(fieldValue, ",")
	if len(splitValue) != 3 {
		return
	}

	var calls float64
	var usecTotal float64
	var err error
	if calls, err = extractVal(splitValue[0]); err != nil {
		return
	}
	if usecTotal, err = extractVal(splitValue[1]); err != nil {
		return
	}

	cmd := splitKey[1]
	e.registerMetricValue(ch, "commands_total", calls, prometheus.CounterValue, []string{cmd})
	e.registerMetricValue(ch, "commands_duration_seconds_total", usecTotal/1e6, prometheus.CounterValue, []string{cmd})
}

func (e *Exporter) handleMetricsReplication(ch chan<- prometheus.Metric, host RedisHost, fieldKey string, fieldValue string) bool {
	// only slaves have this field
	if fieldKey == "master_link_status" {
		if fieldValue == "up" {
			e.registerGaugeValue(ch, "master_link_up", 1, []string{})
		} else {
			e.registerGaugeValue(ch, "master_link_up", 0, []string{})
		}
		return true
	}

	// not a slave, try extracting master metrics
	if slaveOffset, slaveIP, slavePort, slaveState, slaveLag, ok := parseConnectedSlaveString(fieldKey, fieldValue); ok {
		e.registerGaugeValue(ch, "connected_slave_offset", slaveOffset, []string{slaveIP, slavePort, slaveState})

		if slaveLag > -1 {
			e.registerGaugeValue(ch, "connected_slave_lag_seconds", slaveLag, []string{slaveIP, slavePort, slaveState})
		}
		return true
	}

	return false
}

func (e *Exporter) handleMetricsServer(ch chan<- prometheus.Metric, host RedisHost, fieldKey string, fieldValue string) {
	if fieldKey == "uptime_in_seconds" {
		if uptime, err := strconv.ParseFloat(fieldValue, 64); err == nil {
			e.registerGaugeValue(ch, "start_time_seconds", float64(time.Now().Unix())-uptime, []string{})
		}
	}
}

func (e *Exporter) extractInfoMetrics(ch chan<- prometheus.Metric, host RedisHost, info string, dbCount int) error {
	instanceInfo := map[string]string{}
	slaveInfo := map[string]string{}
	handledDBs := map[string]bool{}

	fieldClass := ""
	lines := strings.Split(info, "\r\n")
	for _, line := range lines {
		log.Debugf("info: %s", line)
		if len(line) > 0 && line[0] == '#' {
			fieldClass = line[2:]
			continue
		}

		if (len(line) < 2) || (!strings.Contains(line, ":")) {
			continue
		}

		split := strings.SplitN(line, ":", 2)
		fieldKey := split[0]
		fieldValue := split[1]

		if _, ok := instanceInfoFields[fieldKey]; ok {
			instanceInfo[fieldKey] = fieldValue
			continue
		}

		if _, ok := slaveInfoFields[fieldKey]; ok {
			slaveInfo[fieldKey] = fieldValue
			continue
		}

		switch fieldClass {

		case "Replication":
			if ok := e.handleMetricsReplication(ch, host, fieldKey, fieldValue); ok {
				continue
			}

		case "Server":
			e.handleMetricsServer(ch, host, fieldKey, fieldValue)

		case "Commandstats":
			e.handleMetricsCommandStats(ch, host, fieldKey, fieldValue)
			continue

		case "Keyspace":
			if keysTotal, keysEx, avgTTL, ok := parseDBKeyspaceString(fieldKey, fieldValue); ok {
				dbName := fieldKey

				e.registerGaugeValue(ch, "db_keys", keysTotal, []string{dbName})
				e.registerGaugeValue(ch, "db_keys_expiring", keysEx, []string{dbName})

				if avgTTL > -1 {
					e.registerGaugeValue(ch, "db_avg_ttl_seconds", avgTTL, []string{dbName})
				}
				handledDBs[dbName] = true
				continue
			}
		}

		if !includeMetric(fieldKey) {
			continue
		}

		e.parseAndRegisterMetric(ch, host, fieldKey, fieldValue)
	}

	for dbIndex := 0; dbIndex < dbCount; dbIndex++ {
		dbName := "db" + strconv.Itoa(dbIndex)
		if _, exists := handledDBs[dbName]; !exists {
			e.registerGaugeValue(ch, "db_keys", 0, []string{dbName})
			e.registerGaugeValue(ch, "db_keys_expiring", 0, []string{dbName})
		}
	}

	e.registerGaugeValue(ch, "instance_info", 1, []string{
		instanceInfo["role"],
		instanceInfo["redis_version"],
		instanceInfo["redis_build_id"],
		instanceInfo["redis_mode"],
		instanceInfo["os"]})

	if instanceInfo["role"] == "slave" {
		e.registerGaugeValue(ch, "slave_info", 1, []string{
			slaveInfo["master_host"],
			slaveInfo["master_port"],
			slaveInfo["slave_read_only"]})
	}

	return nil
}

func (e *Exporter) extractClusterInfoMetrics(ch chan<- prometheus.Metric, host RedisHost, info string) error {
	lines := strings.Split(info, "\r\n")

	for _, line := range lines {
		log.Debugf("info: %s", line)

		split := strings.Split(line, ":")
		if len(split) != 2 {
			continue
		}
		fieldKey := split[0]
		fieldValue := split[1]

		if !includeMetric(fieldKey) {
			continue
		}

		e.parseAndRegisterMetric(ch, host, fieldKey, fieldValue)
	}

	return nil
}

func (e *Exporter) parseAndRegisterMetric(ch chan<- prometheus.Metric, host RedisHost, fieldKey, fieldValue string) error {
	orgMetricName := sanitizeMetricName(fieldKey)
	metricName := orgMetricName
	if newName, ok := metricMapGauges[metricName]; ok {
		metricName = newName
	} else {
		if newName, ok := metricMapCounters[metricName]; ok {
			metricName = newName
		}
	}

	var err error
	var val float64

	switch fieldValue {

	case "ok":
		val = 1

	case "err", "fail":
		val = 0

	default:
		val, err = strconv.ParseFloat(fieldValue, 64)

	}
	if err != nil {
		log.Debugf("couldn't parse %s, err: %s", fieldValue, err)
	}

	t := prometheus.GaugeValue
	if metricMapCounters[orgMetricName] != "" {
		t = prometheus.CounterValue
	}
	e.registerMetricValue(ch, metricName, val, t, []string{})

	return nil
}

func doRedisCmd(c redis.Conn, cmd string, args ...interface{}) (reply interface{}, err error) {
	log.Debugf("c.Do() - running command: %s %s", cmd, args)
	defer log.Debugf("c.Do() - done")
	res, err := c.Do(cmd, args...)
	if err != nil {
		log.Debugf("c.Do() - err: %s", err)
	}
	return res, err
}

var errNotFound = errors.New("key not found")

// getKeyInfo takes a key and returns the type, and the size or length of the value stored at that key.
func getKeyInfo(c redis.Conn, key string) (info keyInfo, err error) {

	if info.keyType, err = redis.String(c.Do("TYPE", key)); err != nil {
		return info, err
	}

	switch info.keyType {
	case "none":
		return info, errNotFound
	case "string":
		if size, err := redis.Int64(c.Do("PFCOUNT", key)); err == nil {
			info.keyType = "hyperloglog"
			info.size = float64(size)
		} else if size, err := redis.Int64(c.Do("STRLEN", key)); err == nil {
			info.size = float64(size)
		}
	case "list":
		if size, err := redis.Int64(c.Do("LLEN", key)); err == nil {
			info.size = float64(size)
		}
	case "set":
		if size, err := redis.Int64(c.Do("SCARD", key)); err == nil {
			info.size = float64(size)
		}
	case "zset":
		if size, err := redis.Int64(c.Do("ZCARD", key)); err == nil {
			info.size = float64(size)
		}
	case "hash":
		if size, err := redis.Int64(c.Do("HLEN", key)); err == nil {
			info.size = float64(size)
		}
	case "stream":
		if size, err := redis.Int64(c.Do("XLEN", key)); err == nil {
			info.size = float64(size)
		}
	default:
		err = fmt.Errorf("Unknown type: %v for key: %v", info.keyType, key)
	}

	return info, err
}

// scanForKeys returns a list of keys matching `pattern` by using `SCAN`, which is safer for production systems than using `KEYS`.
// This function was adapted from: https://github.com/reisinger/examples-redigo
func scanForKeys(c redis.Conn, pattern string) ([]string, error) {
	iter := 0
	keys := []string{}

	for {
		arr, err := redis.Values(c.Do("SCAN", iter, "MATCH", pattern))
		if err != nil {
			return keys, fmt.Errorf("error retrieving '%s' keys err: %s", pattern, err)
		}
		if len(arr) != 2 {
			return keys, fmt.Errorf("invalid response from SCAN for pattern: %s", pattern)
		}

		k, _ := redis.Strings(arr[1], nil)
		keys = append(keys, k...)

		if iter, _ = redis.Int(arr[0], nil); iter == 0 {
			break
		}
	}

	return keys, nil
}

// getKeysFromPatterns does a SCAN for a key if the key contains pattern characters
func getKeysFromPatterns(c redis.Conn, keys []dbKeyPair) (expandedKeys []dbKeyPair, err error) {
	expandedKeys = []dbKeyPair{}
	for _, k := range keys {
		if regexp.MustCompile(`[\?\*\[\]\^]+`).MatchString(k.key) {
			_, err := c.Do("SELECT", k.db)
			if err != nil {
				return expandedKeys, err
			}
			keyNames, err := scanForKeys(c, k.key)
			if err != nil {
				log.Errorf("error with SCAN for pattern: %#v err: %s", k.key, err)
				continue
			}
			for _, keyName := range keyNames {
				expandedKeys = append(expandedKeys, dbKeyPair{db: k.db, key: keyName})
			}
		} else {
			expandedKeys = append(expandedKeys, k)
		}
	}
	return expandedKeys, err
}

func (e *Exporter) scrapeRedisHost(ch chan<- prometheus.Metric, host RedisHost) error {
	options := []redis.DialOption{
		redis.DialConnectTimeout(5 * time.Second),
		redis.DialReadTimeout(5 * time.Second),
		redis.DialWriteTimeout(5 * time.Second),

		redis.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: e.options.SkipTLSVerification,
		}),
	}

	if host.Password != "" {
		options = append(options, redis.DialPassword(host.Password))
	}

	log.Debugf("Trying DialURL(): %s", host.Addr)
	c, err := redis.DialURL(host.Addr, options...)

	if err != nil {
		log.Debugf("DialURL() failed, err: %s", err)
		if frags := strings.Split(host.Addr, "://"); len(frags) == 2 {
			log.Debugf("Trying: Dial(): %s %s", frags[0], frags[1])
			c, err = redis.Dial(frags[0], frags[1], options...)
		} else {
			log.Debugf("Trying: Dial(): tcp %s", host.Addr)
			c, err = redis.Dial("tcp", host.Addr, options...)
		}
	}

	if err != nil {
		log.Debugf("aborting for addr: %s - redis err: %s", host.Addr, err)
		return err
	}

	defer c.Close()
	log.Debugf("connected to: %s", host.Addr)

	dbCount := 0

	if config, err := redis.Strings(c.Do(e.options.ConfigCommandName, "GET", "*")); err == nil {
		dbCount, err = e.extractConfigMetrics(ch, config, host)
		if err != nil {
			log.Errorf("Redis CONFIG err: %s", err)
			return err
		}
	} else {
		log.Debugf("Redis CONFIG err: %s", err)
	}

	infoAll, err := redis.String(doRedisCmd(c, "INFO", "ALL"))
	if err != nil {
		infoAll, err = redis.String(doRedisCmd(c, "INFO"))
		if err != nil {
			log.Errorf("Redis INFO err: %s", err)
			return err
		}
	}
	isClusterEnabled := strings.Contains(infoAll, "cluster_enabled:1")

	if isClusterEnabled {
		if clusterInfo, err := redis.String(doRedisCmd(c, "CLUSTER", "INFO")); err == nil {
			e.extractClusterInfoMetrics(ch, host, clusterInfo)

			// in cluster mode Redis only supports one database so no extra padding beyond that needed
			dbCount = 1
		} else {
			log.Errorf("Redis CLUSTER INFO err: %s", err)
		}
	} else {
		// in non-cluster mode, if dbCount is zero then "CONFIG" failed to retrieve a valid
		// number of databases and we use the Redis config default which is 16
		if dbCount == 0 {
			dbCount = 16
		}
	}

	e.extractInfoMetrics(ch, host, infoAll, dbCount)

	// SERVER command only works on tile38 database. check the following link to
	// find out more: https://tile38.com/
	if serverInfo, err := redis.Strings(doRedisCmd(c, "SERVER")); err == nil {
		e.extractTile38Metrics(ch, serverInfo, host)
	} else {
		log.Debugf("Tile38 SERVER err: %s", err)
	}

	if reply, err := doRedisCmd(c, "LATENCY", "LATEST"); err == nil {
		var eventName string
		if tempVal, _ := reply.([]interface{}); len(tempVal) > 0 {
			latencyResult := tempVal[0].([]interface{})
			var spikeLast, spikeDuration, max int64
			if _, err := redis.Scan(latencyResult, &eventName, &spikeLast, &spikeDuration, &max); err == nil {
				spikeDuration = spikeDuration / 1e6
				e.registerGaugeValue(ch, "latency_spike_last", float64(spikeLast), []string{eventName})
				e.registerGaugeValue(ch, "latency_spike_seconds", float64(spikeDuration), []string{eventName})
			}
		}
	}

	log.Debugf("e.singleKeys: %#v", e.singleKeys)
	allKeys := append([]dbKeyPair{}, e.singleKeys...)

	log.Debugf("e.keys: %#v", e.keys)
	scannedKeys, err := getKeysFromPatterns(c, e.keys)
	if err != nil {
		log.Errorf("Error expanding key patterns: %#v", err)
	} else {
		allKeys = append(allKeys, scannedKeys...)
	}

	log.Debugf("allKeys: %#v", allKeys)
	for _, k := range allKeys {
		if _, err := doRedisCmd(c, "SELECT", k.db); err != nil {
			log.Debugf("Couldn't select database %#v when getting key info.", k.db)
			continue
		}

		info, err := getKeyInfo(c, k.key)
		if err != nil {
			switch err {
			case errNotFound:
				log.Debugf("Key '%s' not found when trying to get type and size.", k.key)
			default:
				log.Error(err)
			}
			continue
		}
		dbLabel := "db" + k.db

		e.registerGaugeValue(ch, "key_sizes", info.size, []string{dbLabel, k.key})

		// Only record value metric if value is float-y
		if val, err := redis.Float64(c.Do("GET", k.key)); err == nil {
			e.registerGaugeValue(ch, "key_values", val, []string{dbLabel, k.key})
		}
	}

	if e.LuaScript != nil && len(e.LuaScript) > 0 {
		log.Debug("e.script")
		kv, err := redis.StringMap(doRedisCmd(c, "EVAL", e.LuaScript, 0, 0))
		if err != nil {
			log.Errorf("Collect script error: %v", err)
		} else if kv != nil {
			for key, stringVal := range kv {
				if val, err := strconv.ParseFloat(stringVal, 64); err == nil {
					e.registerGaugeValue(ch, "script_values", val, []string{key})
				}
			}
		}
	}

	if reply, err := c.Do("SLOWLOG", "LEN"); err == nil {
		e.registerGaugeValue(ch, "slowlog_length", float64(reply.(int64)), []string{})
	}

	if values, err := redis.Values(c.Do("SLOWLOG", "GET", "1")); err == nil {
		var slowlogLastID int64
		var lastSlowExecutionDurationSeconds float64

		if len(values) > 0 {
			if values, err = redis.Values(values[0], err); err == nil && len(values) > 0 {
				slowlogLastID = values[0].(int64)
				if len(values) > 2 {
					lastSlowExecutionDurationSeconds = float64(values[2].(int64)) / 1e6
				}
			}
		}

		e.registerGaugeValue(ch, "slowlog_last_id", float64(slowlogLastID), []string{})
		e.registerGaugeValue(ch, "last_slow_execution_duration_seconds", lastSlowExecutionDurationSeconds, []string{})
	}

	log.Debugf("scrapeRedisHost() done")
	return nil
}

func (e *Exporter) scrapeHost(ch chan<- prometheus.Metric, host RedisHost) {
	now := time.Now().UnixNano()
	var up float64 = 1
	if err := e.scrapeRedisHost(ch, host); err != nil {
		up = 0
		e.registerGaugeValue(ch, "scrape_error", 1.0, []string{fmt.Sprintf("%s", err)})
	} else {
		e.registerGaugeValue(ch, "scrape_error", 0, []string{""})
	}

	e.registerGaugeValue(ch, "up", up, []string{})
	e.registerGaugeValue(ch, "last_scrape_duration", float64(time.Now().UnixNano()-now)/1000000000, []string{})
}

func (e *Exporter) scrapeAllHosts(ch chan<- prometheus.Metric) {
	e.totalScrapes.Inc()
	wg := sync.WaitGroup{}
	wg.Add(len(e.hosts))
	for _, host := range e.hosts {
		go func(h RedisHost) {
			e.scrapeHost(ch, h)
			wg.Done()
		}(host)
	}
	wg.Wait()
}
