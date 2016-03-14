package main

import (
	"bufio"
	container "container/heap"
	encoding "encoding/csv"
	"flag"
	"fmt"
	"github.com/wildducktheories/go-csv"
	"github.com/wildducktheories/influx-log-analyser/record"
	"github.com/wildducktheories/timeserieslog"
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
					endrec.Put(record.REQUEST_ID, orec.Get(record.REQUEST_ID))

					w.Write(endrec)
				}
			}
			w.Write(orec)
		}

		return r.Error()
	}()
}

type sortingProcess struct {
}

type sortElement struct {
	timestamp int64
	ordinal   int
	record    csv.Record
}

func (e *sortElement) Less(other tsl.Element) bool {
	otherE := other.(*sortElement)
	if e.timestamp == otherE.timestamp {
		return e.ordinal < otherE.ordinal
	} else {
		return e.timestamp < otherE.timestamp
	}
}

func (p *sortingProcess) Run(r csv.Reader, b csv.WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		defer r.Close()

		w := b(r.Header())
		defer w.Close(err)

		buffer := tsl.NewUnsortedRange()

		dump := func(r tsl.SortedRange) {
			c := r.Open()
			for {
				n := c.Next()
				if n == nil {
					return
				}
				w.Write(n.(*sortElement).record)
			}
		}

		window := time.Hour
		var snap time.Time
		count := 1
		keep := tsl.EmptyRange
		for i := range r.C() {
			count++
			if ts, err := time.Parse("2006-01-02 15:04:05", i.Get("startedAt")); err != nil {
				continue
			} else {
				if count == 2 {
					snap = ts.Add(window)
				}
				element := &sortElement{
					timestamp: ts.UnixNano(),
					ordinal:   count,
					record:    i,
				}
				buffer.Add([]tsl.Element{element})
				if ts.Sub(snap) > 0 {
					write, hold := buffer.Freeze().Partition(&sortElement{
						timestamp: ts.Add(-window).UnixNano(),
						ordinal:   0,
						record:    nil,
					}, tsl.LessOrder)
					buffer = tsl.NewUnsortedRange()
					concat := tsl.Merge(keep, write)
					keep = hold
					dump(concat)
				}
			}
		}
		concat := tsl.Merge(keep, buffer.Freeze())
		dump(concat)
		return r.Error()
	}()
}

type countingProcess struct {
}

type request struct {
	id      string
	unix    int64
	details record.Record
}

type requestHeap struct {
	data   []*request
	length int
}

func (h *requestHeap) Less(i, j int) bool {
	return h.data[i].unix < h.data[j].unix
}

func (h *requestHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *requestHeap) Len() int {
	return h.length
}

func (h *requestHeap) Push(i interface{}) {
	if h.length < len(h.data) {
		h.data[h.length] = i.(*request)
	} else {
		h.data = append(h.data, i.(*request))
	}
	h.length++
}

func (h *requestHeap) Pop() interface{} {
	r := h.data[h.length-1]
	h.data[h.length-1] = nil
	h.length--
	return r
}

func (h *requestHeap) find(id string) int {
	for i, r := range h.data {
		if i >= h.length {
			break
		}
		if r.id == id {
			return i
		}
	}
	return -1
}

func (p *countingProcess) Run(r csv.Reader, b csv.WriterBuilder, errCh chan<- error) {
	errCh <- func() (err error) {
		defer r.Close()

		w := b(extendHeader(r.Header(), []string{"active", "oldest"}))
		active := 0
		defer w.Close(err)
		heap := &requestHeap{data: []*request{}}
		container.Init(heap)

		for i := range r.C() {

			eventType := i.Get("eventType")
			start := eventType == "start"
			end := eventType == "end"
			oldest := ""

			details := record.NewRecord()
			details.Decode(i)

			if heap.length > 0 {
				oldest = heap.data[0].id
			}

			if start {
				req := &request{id: details.RequestId(), unix: details.StartTimestamp().UnixNano(), details: details}
				container.Push(heap, req)
			} else if end {
				x := heap.find(details.RequestId())
				if x >= 0 {
					_ = container.Remove(heap, x).(*request)
				}
			}

			o := w.Blank()
			o.PutAll(i)
			o.Put("active", strconv.FormatInt(int64(active), 10))
			o.Put("oldest", oldest)
			if e := w.Write(o); e != nil {
				return e
			}
			if start {
				active += 1
			} else if end {
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
		w := b(extendHeader(record.CsvHeaders, []string{"active", "oldest"}))
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
	parseOnly := false
	noParse := false
	sortOnly := false
	comma := ","
	flag.BoolVar(&tabs, "tabs", false, "Use tabs as the output delimiter.")
	flag.BoolVar(&parseOnly, "parse-only", false, "Convert the file into CSV without analysis")
	flag.BoolVar(&noParse, "no-parse", false, "Assume stdin contains the result of a previous --parse-only run.")
	flag.BoolVar(&sortOnly, "sort-only", false, "Sort the stream by startedAt.")
	flag.Parse()

	if tabs {
		comma = "\t"
	}

	if noParse && parseOnly {
		fmt.Fprintf(os.Stderr, "at most one of --parse-only and --no-parse may be specified\n")
		os.Exit(1)
	}

	csvEncoder := encoding.NewWriter(os.Stdout)
	csvEncoder.Comma = rune(comma[0])

	if !parseOnly {

		var pipeline csv.Process

		if sortOnly {
			pipeline = &sortingProcess{}
		} else {
			pipeline = csv.NewPipeLine([]csv.Process{
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
		}

		var in csv.Reader
		if noParse {
			in = csv.WithIoReader(os.Stdin)
		} else {
			pipe := csv.NewPipe()
			go parse(os.Stdin, pipe.Builder())
			in = pipe.Reader()
		}
		errCh := make(chan error, 1)
		pipeline.Run(in, csv.WithCsvWriter(csvEncoder, os.Stdout), errCh)
		err := <-errCh
		if err != nil {
			fmt.Fprintf(os.Stderr, "fatal: %s\n", err)
			os.Exit(1)
		}
	} else {
		parse(os.Stdin, csv.WithCsvWriter(csvEncoder, os.Stdout))
	}
}
