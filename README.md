# cgroupv2_exporter
Prometheus exporter for Cgroup v2 metrics, written in Go with pluggable metric collectors similar to node_exporter.

## Installation and Usage
The `cgroupv2_exporter` listens on HTTP port 9100 by default. See the `--help` output for more options.

## Collectors

Collectors are enabled by providing a `--collector.<name>` flag.
Collectors that are enabled by default can be disabled by providing a `--no-collector.<name>` flag.
To enable only some specific collector(s), use `--collector.disable-defaults --collector.<name> ...`.

### Enabled by default
Name     | Description
---------|-------------
memory.current | 
memory.swap.current | 
memory.high | 
memory.pressure | 

### Disabled by default
Name     | Description
---------|-------------
memory.stat | 
