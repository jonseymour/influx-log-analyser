package main

import (
	"bufio"
	encoding "encoding/csv"
	"flag"
	"fmt"
	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/influx-log-analyser/record"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func parse(input io.ReadCloser, builder csv.WriterBuilder) {
	output := builder(record.CsvHeaders)
	lines := bufio.NewReader(input)
	for {
		if line, err := lines.ReadString('\n'); err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		} else {
			line = strings.TrimSpace(line)
			if r, err := record.ParseInfluxLogLine(line); err != nil {
				if err != record.ErrNotHttp && err != record.ErrInvalidNumberOfTokens {
					fmt.Fprintf(os.Stderr, "%v\n", err)
				}
			} else {
				cr := output.Blank()
				r.Encode(cr)
				output.Write(cr)
			}
		}
	}
	output.Close(nil)
}

type splitProcess struct {
}

func extendHeader(h []string, x []string) []string {
	o := make([]string, len(h)+len(x))
	copy(o, h)
	copy(o[len(h):], x)
	return o
}

func (p *splitProcess) Run(r csv.Reader, b csv.WriterBuilder, errCh chan<- error) {

	errCh <- func() (err error) {
		defer r.Close()

		w := b(extendHeader(r.Header(), []string{"ordinal", "unix", "eventType"}))

		defer w.Close(err)

		i := 0
		for rec := range r.C() {
			i++
			orec := w.Blank()
			endrec := w.Blank()

			orec.PutAll(rec)

			ordinal := fmt.Sprintf("%d", i)
			orec.Put("ordinal", ordinal)
			endrec.Put("ordinal", ordinal)

			if t, err := time.Parse("2006-01-02 15:04:05", orec.Get("startedAt")); err == nil {
				startT := t.UnixNano() / int64(time.Millisecond)
				if duration, err := strconv.ParseInt(orec.Get("duration"), 10, 64); err == nil {
					endT := startT + duration

					orec.Put("unix", strconv.FormatInt(startT, 10))
					endrec.Put("unix", strconv.FormatInt(endT, 10))
					orec.Put("eventType", "start")
					endrec.Put("eventType", "end")

					w.Write(endrec)
				}
			}
			w.Write(orec)
		}

		return r.Error()
	}()
}

type countingProcess struct {
}

func (p *countingProcess) Run(r csv.Reader, b csv.WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		defer r.Close()

		w := b(extendHeader(r.Header(), []string{"active"}))
		active := 0
		defer w.Close(err)
		for i := range r.C() {
			o := w.Blank()
			o.PutAll(i)
			o.Put("active", strconv.FormatInt(int64(active), 10))
			if e := w.Write(o); e != nil {
				return e
			}
			eventType := i.Get("eventType")
			switch eventType {
			case "start":
				active += 1
			case "end":
				active -= 1
			}
		}
		return r.Error()
	}()
}

type filterProcess struct {
}

func (p *filterProcess) Run(r csv.Reader, b csv.WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		defer r.Close()
		w := b(extendHeader(record.CsvHeaders, []string{"active"}))
		defer w.Close(err)
		for i := range r.C() {
			if i.Get("eventType") == "end" {
				continue
			}
			o := w.Blank()
			o.PutAll(i)
			if e := w.Write(o); e != nil {
				return e
			}
		}
		return r.Error()
	}()
}

func main() {

	tabs := false
	comma := ","
	flag.BoolVar(&tabs, "tabs", false, "Use tabs as the output delimiter.")
	flag.Parse()

	if tabs {
		comma = "\t"
	}

	csvEncoder := encoding.NewWriter(os.Stdout)
	csvEncoder.Comma = rune(comma[0])
	pipe := csv.NewPipe()
	pipeline := csv.NewPipeLine([]csv.Process{
		&splitProcess{},
		(&csv.SortKeys{
			Keys:    []string{"unix", "ordinal"},
			Numeric: []string{"unix", "ordinal"},
		}).AsSortProcess(),
		&countingProcess{},
		(&csv.SortKeys{
			Keys:    []string{"ordinal"},
			Numeric: []string{"ordinal"},
		}).AsSortProcess(),
		&filterProcess{},
	})

	go parse(os.Stdin, pipe.Builder())
	errCh := make(chan error, 1)
	pipeline.Run(pipe.Reader(), csv.WithCsvWriter(csvEncoder, os.Stdout), errCh)
	err := <-errCh

	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %s\n", err)
		os.Exit(1)
	}
}
