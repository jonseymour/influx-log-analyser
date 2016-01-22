#NAME
influx-log-analyser - parser for influx log entries

#DESCRIPTION
Converts [http] lines found in an influxd log stream into a structured CSV stream containing the same information.

#USAGE

```
cat influx.log | influx-log-analyzer
```

To get tab separated output instead of comma separated output, use the --tabs option.

To get a JSON output instead of a CSV output, run:

```
go get -d github.com/wildducktheories/go-csv/csv-to-json &&
go install github.com/wildducktheories/go-csv/csv-to-json
```

then run:

```
cat influx.log | influx-log-analyzer | csv-to-json
```








