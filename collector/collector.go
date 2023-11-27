package collector

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/flipkart-incubator/cgroupv2_exporter/parsers"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

// Namespace defines the common namespace to be used by all metrics.
const namespace = "cgroupv2"

var (
	scrapeDurationDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_duration_seconds"),
		"cgroupv2_exporter: Duration of a collector scrape.",
		[]string{"collector"},
		nil,
	)
	scrapeSuccessDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "scrape", "collector_success"),
		"cgroupv2_exporter: Whether a collector succeeded.",
		[]string{"collector"},
		nil,
	)
)

const (
	defaultEnabled  = true
	defaultDisabled = false
)

var (
	factories              = make(map[string]func(logger log.Logger, cgroups []string) (Collector, error))
	initiatedCollectorsMtx = sync.Mutex{}
	initiatedCollectors    = make(map[string]Collector)
	collectorState         = make(map[string]*bool)
	forcedCollectors       = map[string]bool{} // collectors which have been explicitly enabled or disabled
)

func registerCollector(collector string, isDefaultEnabled bool, factory func(logger log.Logger, cgroups []string) (Collector, error)) {
	var helpDefaultState string
	if isDefaultEnabled {
		helpDefaultState = "enabled"
	} else {
		helpDefaultState = "disabled"
	}

	flagName := fmt.Sprintf("collector.%s", collector)
	flagHelp := fmt.Sprintf("Enable the %s collector (default: %s).", collector, helpDefaultState)
	defaultValue := fmt.Sprintf("%v", isDefaultEnabled)

	flag := kingpin.Flag(flagName, flagHelp).Default(defaultValue).Action(collectorFlagAction(collector)).Bool()
	collectorState[collector] = flag

	factories[collector] = factory
}

type Cgroup2Collector struct {
	Collectors map[string]Collector
	logger     log.Logger
}

type Cgroupv2FileCollector struct {
	gaugeVecs map[string]*prometheus.GaugeVec
	parser    parsers.Parser
	dirNames  []string
	fileName  string
	logger    log.Logger
}

// DisableDefaultCollectors sets the collector state to false for all collectors which
// have not been explicitly enabled on the command line.
func DisableDefaultCollectors() {
	for c := range collectorState {
		if _, ok := forcedCollectors[c]; !ok {
			*collectorState[c] = false
		}
	}
}

// collectorFlagAction generates a new action function for the given collector
// to track whether it has been explicitly enabled or disabled from the command line.
// A new action function is needed for each collector flag because the ParseContext
// does not contain information about which flag called the action.
// See: https://github.com/alecthomas/kingpin/issues/294
func collectorFlagAction(collector string) func(ctx *kingpin.ParseContext) error {
	return func(ctx *kingpin.ParseContext) error {
		forcedCollectors[collector] = true
		return nil
	}
}

func NewCgroupv2Collector(cgroups []string, logger log.Logger, filters ...string) (*Cgroup2Collector, error) {
	f := make(map[string]bool)
	for _, filter := range filters {
		enabled, exist := collectorState[filter]
		if !exist {
			return nil, fmt.Errorf("missing collector: %s", filter)
		}
		if !*enabled {
			return nil, fmt.Errorf("disabled collector: %s", filter)
		}
		f[filter] = true
	}
	collectors := make(map[string]Collector)
	initiatedCollectorsMtx.Lock()
	defer initiatedCollectorsMtx.Unlock()
	for key, enabled := range collectorState {
		if !*enabled || (len(f) > 0 && !f[key]) {
			continue
		}
		if collector, ok := initiatedCollectors[key]; ok {
			collectors[key] = collector
		} else {
			collector, err := factories[key](log.With(logger, "collector", key), cgroups)
			if err != nil {
				return nil, err
			}
			collectors[key] = collector
			initiatedCollectors[key] = collector
		}
	}
	return &Cgroup2Collector{Collectors: collectors, logger: logger}, nil
}

func (cgc *Cgroup2Collector) Describe(ch chan<- *prometheus.Desc) {
	// Describe is required for the prometheus.Collector interface but is not used in this project.
}

func sanitizeP8sName(name string) string {
	// Noticed some cgroup names with escape sequence like \x2d. Clean them up.
	if unquoted, err := strconv.Unquote(`"` + name + `"`); err == nil {
		name = unquoted
	}

	// Use a regular expression to replace unsupported characters with underscores
	regex := regexp.MustCompile(`[^a-zA-Z0-9_:]`)
	name = regex.ReplaceAllString(name, "_")

	// squeeze underscore repeats
	regex = regexp.MustCompile(`_+`)
	name = regex.ReplaceAllString(name, "_")

	name = strings.Trim(name, "_")

	return name
}

// Collect implements the prometheus.Collector interface.
func (cgc *Cgroup2Collector) Collect(ch chan<- prometheus.Metric) {
	wg := sync.WaitGroup{}
	wg.Add(len(cgc.Collectors))
	for name, c := range cgc.Collectors {
		go func(name string, c Collector) {
			execute(name, c, ch, cgc.logger)
			wg.Done()
		}(name, c)
	}
	wg.Wait()
}

func execute(name string, c Collector, ch chan<- prometheus.Metric, logger log.Logger) {
	begin := time.Now()
	err := c.Update(ch)
	duration := time.Since(begin)
	var success float64

	if err != nil {
		if IsNoDataError(err) {
			level.Debug(logger).Log("msg", "collector returned no data", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		} else {
			level.Error(logger).Log("msg", "collector failed", "name", name, "duration_seconds", duration.Seconds(), "err", err)
		}
		success = 0
	} else {
		level.Debug(logger).Log("msg", "collector succeeded", "name", name, "duration_seconds", duration.Seconds())
		success = 1
	}
	ch <- prometheus.MustNewConstMetric(scrapeDurationDesc, prometheus.GaugeValue, duration.Seconds(), name)
	ch <- prometheus.MustNewConstMetric(scrapeSuccessDesc, prometheus.GaugeValue, success, name)
}

func (cc Cgroupv2FileCollector) Update(ch chan<- prometheus.Metric) error {
	// Use the parser to fetch metrics for the specified file in all cgroup directories
	for _, dirName := range cc.dirNames {
		//level.Info(cc.logger).Log("dir", dirName, "file", cc.fileName)
		filePath := filepath.Join(dirName, cc.fileName)
		file, err := os.Open(filePath)
		if err != nil {
			level.Error(cc.logger).Log("dir", dirName, "err", err)
			return err
		}
		defer file.Close()

		metrics, err := cc.parser.Parse(file)
		if err != nil {
			level.Error(cc.logger).Log("dir", dirName, "err", err)
			return err
		}
		//level.Info(cc.logger).Log("dir", dirName)

		cgroupName := sanitizeP8sName(filepath.Base(dirName))
		// Set the gauge value with the directory label
		for key, value := range metrics {
			metricName := sanitizeP8sName(key)
			if _, ok := cc.gaugeVecs[metricName]; !ok {
				cc.gaugeVecs[metricName] = prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Namespace: "cgroupv2",
						Name:      metricName,
						Help:      fmt.Sprintf("metric %s from file %s", metricName, cc.fileName),
					},
					[]string{"cgroup"}, // Adding cgroup directory as a label
				)
			}
			cc.gaugeVecs[metricName].WithLabelValues(cgroupName).Set(value)
			// Collect the metric
			cc.gaugeVecs[metricName].Collect(ch)
			level.Debug(cc.logger).Log("msg", fmt.Sprintf("collected metric: %s value: %f cgroup: %s", metricName, value, cgroupName))
		}
	}
	return nil
}

// Collector is the interface a collector has to implement.
type Collector interface {
	// Get new metrics and expose them via prometheus registry.
	Update(ch chan<- prometheus.Metric) error
}

// ErrNoData indicates the collector found no data to collect, but had no other error.
var ErrNoData = errors.New("collector returned no data")

func IsNoDataError(err error) bool {
	return err == ErrNoData
}

func init() {
	registerCollector("memory.pressure", defaultEnabled, NewMemoryPressureCollector)
	registerCollector("memory.current", defaultEnabled, NewMemoryCurrentCollector)
	registerCollector("memory.swap.current", defaultEnabled, NewMemorySwapCurrentCollector)
	registerCollector("memory.high", defaultEnabled, NewMemoryHighCollector)

	registerCollector("memory.stat", defaultDisabled, NewMemoryStatCollector)
}

func NewMemoryPressureCollector(logger log.Logger, cgroups []string) (Collector, error) {
	file := "memory.pressure"
	return &Cgroupv2FileCollector{
		gaugeVecs: make(map[string]*prometheus.GaugeVec),
		parser: &parsers.NestedKeyValueParser{
			MetricPrefix: sanitizeP8sName(file),
			Logger:       log.With(logger, "file", file),
		},
		dirNames: cgroups,
		fileName: file,
		logger:   log.With(logger, "file", file),
	}, nil
}
func NewMemoryCurrentCollector(logger log.Logger, cgroups []string) (Collector, error) {
	file := "memory.current"
	return &Cgroupv2FileCollector{
		gaugeVecs: make(map[string]*prometheus.GaugeVec),
		parser: &parsers.SingleValueParser{
			MetricPrefix: sanitizeP8sName(file),
			Logger:       log.With(logger, "file", file),
		},
		dirNames: cgroups,
		fileName: file,
		logger:   log.With(logger, "file", file),
	}, nil
}
func NewMemorySwapCurrentCollector(logger log.Logger, cgroups []string) (Collector, error) {
	file := "memory.swap.current"
	return &Cgroupv2FileCollector{
		gaugeVecs: make(map[string]*prometheus.GaugeVec),
		parser: &parsers.SingleValueParser{
			MetricPrefix: sanitizeP8sName(file),
			Logger:       log.With(logger, "file", file),
		},
		dirNames: cgroups,
		fileName: file,
		logger:   log.With(logger, "file", file),
	}, nil
}
func NewMemoryHighCollector(logger log.Logger, cgroups []string) (Collector, error) {
	file := "memory.high"
	return &Cgroupv2FileCollector{
		gaugeVecs: make(map[string]*prometheus.GaugeVec),
		parser: &parsers.SingleValueParser{
			MetricPrefix: sanitizeP8sName(file),
			Logger:       log.With(logger, "file", file),
		},
		dirNames: cgroups,
		fileName: file,
		logger:   log.With(logger, "file", file),
	}, nil
}
func NewMemoryStatCollector(logger log.Logger, cgroups []string) (Collector, error) {
	file := "memory.stat"
	return &Cgroupv2FileCollector{
		gaugeVecs: make(map[string]*prometheus.GaugeVec),
		parser: &parsers.FlatKeyValueParser{
			MetricPrefix: sanitizeP8sName(file),
			Logger:       log.With(logger, "file", file),
		},
		dirNames: cgroups,
		fileName: file,
		logger:   log.With(logger, "file", file),
	}, nil
}
