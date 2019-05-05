package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"

	"github.com/oliver006/redis_exporter/exporter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	// BuildVersion, BuildDate, BuildCommitSha are filled in by the build script
	BuildVersion   = "<<< filled in by build >>>"
	BuildDate      = "<<< filled in by build >>>"
	BuildCommitSha = "<<< filled in by build >>>"
)

func getEnv(key string, defaultVal string) string {
	if envVal, ok := os.LookupEnv(key); ok {
		return envVal
	}
	return defaultVal
}

func getEnvBool(key string) (envValBool bool) {
	if envVal, ok := os.LookupEnv(key); ok {
		envValBool, _ = strconv.ParseBool(envVal)
	}
	return
}

func main() {
	var (
		redisAddr           = flag.String("redis.addr", getEnv("REDIS_ADDR", ""), "Address of the redis instsance to scrape")
		namespace           = flag.String("namespace", getEnv("REDIS_EXPORTER_NAMESPACE", "redis"), "Namespace for metrics")
		checkKeys           = flag.String("check-keys", getEnv("REDIS_EXPORTER_CHECK_KEYS", ""), "Comma separated list of key-patterns to export value and length/size, searched for with SCAN")
		checkSingleKeys     = flag.String("check-single-keys", getEnv("REDIS_EXPORTER_CHECK_SINGLE_KEYS", ""), "Comma separated list of single keys to export value and length/size")
		scriptPath          = flag.String("script", getEnv("REDIS_EXPORTER_SCRIPT", ""), "Path to Lua Redis script for collecting extra metrics")
		listenAddress       = flag.String("web.listen-address", getEnv("REDIS_EXPORTER_WEB_LISTEN_ADDRESS", ":9121"), "Address to listen on for web interface and telemetry.")
		metricPath          = flag.String("web.telemetry-path", getEnv("REDIS_EXPORTER_WEB_TELEMETRY_PATH", "/metrics"), "Path under which to expose metrics.")
		logFormat           = flag.String("log-format", getEnv("REDIS_EXPORTER_LOG_FORMAT", "txt"), "Log format, valid options are txt and json")
		configCommand       = flag.String("config-command", getEnv("REDIS_EXPORTER_CONFIG_COMMAND", "CONFIG"), "What to use for the CONFIG command")
		isDebug             = flag.Bool("debug", getEnvBool("REDIS_EXPORTER_DEBUG"), "Output verbose debug information")
		showVersion         = flag.Bool("version", false, "Show version information and exit")
		redisMetricsOnly    = flag.Bool("redis-only-metrics", getEnvBool("REDIS_EXPORTER_REDIS_ONLY_METRICS"), "Whether to export go runtime metrics also")
		inclVerbotenMetrics = flag.Bool("incl-verboten-metrics", getEnvBool("REDIS_EXPORTER_INCL_VERBOTEN_METRICS"), "Whether to include metrics that are not considered ok by Prometheus standards")
	)
	flag.Parse()

	switch *logFormat {
	case "json":
		log.SetFormatter(&log.JSONFormatter{})
	default:
		log.SetFormatter(&log.TextFormatter{})
	}
	log.Printf("Redis Metrics Exporter %s    build date: %s    sha1: %s    Go: %s",
		BuildVersion, BuildDate, BuildCommitSha,
		runtime.Version(),
	)
	if *isDebug {
		log.SetLevel(log.DebugLevel)
		log.Debugln("Enabling debug output")
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if *showVersion {
		return
	}

	exp, err := exporter.NewRedisExporter(
		*redisAddr,
		exporter.Options{
			Namespace:              *namespace,
			ConfigCommandName:      *configCommand,
			CheckKeys:              *checkKeys,
			CheckSingleKeys:        *checkSingleKeys,
			IncludeVerbotenMetrics: *inclVerbotenMetrics,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	if *scriptPath != "" {
		if exp.LuaScript, err = ioutil.ReadFile(*scriptPath); err != nil {
			log.Fatalf("Error loading script file %s    err: %s", *scriptPath, err)
		}
	}

	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "redis_exporter_build_info",
		Help: "redis exporter build_info",
	}, []string{"version", "commit_sha", "build_date", "golang_version"})
	buildInfo.WithLabelValues(BuildVersion, BuildCommitSha, BuildDate, runtime.Version()).Set(1)

	if *redisMetricsOnly {
		registry := prometheus.NewRegistry()
		registry.MustRegister(exp)
		handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
		http.Handle(*metricPath, handler)
	} else {
		prometheus.MustRegister(exp)
		prometheus.MustRegister(buildInfo)
		http.Handle(*metricPath, promhttp.Handler())
	}

	http.HandleFunc("/scrape", exp.ScrapeHandler)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
<head><title>Redis Exporter v` + BuildVersion + `</title></head>
<body>
<h1>Redis Exporter ` + BuildVersion + `</h1>
<p><a href='` + *metricPath + `'>Metrics</a></p>
</body>
</html>
`))
	})

	log.Printf("Providing metrics at %s%s", *listenAddress, *metricPath)
	log.Printf("Connecting to redis hosts: %#v", *redisAddr)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
