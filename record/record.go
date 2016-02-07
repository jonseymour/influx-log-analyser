package record

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/wildducktheories/go-csv"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Record interface {
	StartTimestamp() time.Time
	Duration() time.Duration
	IP() string
	User() string
	Method() string
	URL() string
	URLMap() map[string]interface{}
	Protocol() string
	Status() uint
	ContentLength() uint32
	Referrer() string
	UserAgent() string
	RequestId() string
	Decode(r csv.Record) error
	Encode(r csv.Record)
}

type decodedRecord struct {
	startTimestamp time.Time
	duration       time.Duration
	method         string
	ip             string
	user           string
	url            string
	protocol       string
	status         uint
	contentLength  uint32
	referrer       string
	userAgent      string
	requestId      string
	urlMap         map[string]interface{}
}

const (
	STARTED_AT     = "startedAt"
	DURATION       = "duration"
	STATUS         = "status"
	URL            = "url"
	CONTENT_LENGTH = "contentLength"
	IP             = "ip"
	USER           = "user"
	METHOD         = "method"
	PROTOCOL       = "protocol"
	REFERRER       = "referrer"
	REQUEST_ID     = "requestId"
	USER_AGENT     = "userAgent"
)

const (
	TIMESTAMP_FORMAT       = "2006-01-02 15:04:05.000"
	LOG_TIMESTAMP_FORMAT   = "2006/01/02 15:04:05"
	LOG_TIMESTAMP_FORMAT_2 = "02/Jan/2006:15:04:05 -0700"
)

var CsvHeaders = []string{
	STARTED_AT,
	DURATION,
	STATUS,
	URL,
	CONTENT_LENGTH,
	IP,
	USER,
	METHOD,
	PROTOCOL,
	REFERRER,
	REQUEST_ID,
	USER_AGENT,
}

var ErrNotHttp = errors.New("not an http record")
var ErrInvalidNumberOfTokens = errors.New("invalid number of tokens")

func NewRecord() Record {
	return &decodedRecord{}
}

func (r *decodedRecord) StartTimestamp() time.Time {
	return r.startTimestamp
}

func (r *decodedRecord) Duration() time.Duration {
	return r.duration
}

func (r *decodedRecord) Method() string {
	return r.method
}

func (r *decodedRecord) IP() string {
	return r.ip
}

func (r *decodedRecord) User() string {
	return r.user
}

func (r *decodedRecord) URL() string {
	return r.url
}

func (r *decodedRecord) URLMap() map[string]interface{} {
	if r.urlMap == nil {
		r.urlMap = map[string]interface{}{}
		json.Unmarshal([]byte(r.url), r.urlMap)
	}
	return r.urlMap
}

func (r *decodedRecord) Protocol() string {
	return r.protocol
}

func (r *decodedRecord) Status() uint {
	return r.status
}

func (r *decodedRecord) ContentLength() uint32 {
	return r.contentLength
}

func (r *decodedRecord) Referrer() string {
	return r.referrer
}

func (r *decodedRecord) UserAgent() string {
	return r.userAgent
}

func (r *decodedRecord) RequestId() string {
	return r.requestId
}

func (r *decodedRecord) Decode(c csv.Record) error {
	startedAt := c.Get(STARTED_AT)
	if ts, err := time.Parse(TIMESTAMP_FORMAT, startedAt); err == nil {
		r.startTimestamp = ts
	}
	if d, err := strconv.ParseUint(c.Get(DURATION), 10, 64); err == nil {
		r.duration = time.Millisecond * time.Duration(d)
	}
	if l, err := strconv.ParseUint(c.Get(CONTENT_LENGTH), 10, 32); err == nil {
		r.contentLength = uint32(l)
	}
	if s, err := strconv.ParseUint(c.Get(STATUS), 10, 16); err == nil {
		r.status = uint(s)
	}

	r.method = c.Get(METHOD)
	r.ip = c.Get(IP)
	r.user = c.Get(USER)
	r.url = c.Get(URL)
	r.protocol = c.Get(PROTOCOL)
	r.referrer = c.Get(REFERRER)
	r.userAgent = c.Get(USER_AGENT)
	r.requestId = c.Get(REQUEST_ID)

	return nil
}

func urlToEncodedJSON(u *url.URL) string {
	j := map[string]interface{}{}
	j["path"] = u.Path
	if v, err := url.ParseQuery(u.RawQuery); err == nil {
		query := map[string]interface{}{}
		for k, a := range v {
			switch len(a) {
			case 0:
			case 1:
				query[k] = a[0]
			default:
				query[k] = a
			}
		}
		j["query"] = query
	} else {
		j["rawQuery"] = u.RawQuery
	}
	if u.Fragment != "" {
		j["fragment"] = u.Fragment
	}
	if bytes, err := json.Marshal(j); err != nil {
		panic(err)
	} else {
		return string(bytes)
	}
}

// Parses influx log line.
func ParseInfluxLogLine(line string) (Record, error) {

	//[http] 2016/01/03 23:39:23 172.17.0.13 - admin [03/Jan/2016:23:39:22 +0000] GET /query?db=sphere&p=%5BREDACTED%5D&q=select+count%28state%29+from+%22aircon.demandcontrol.state%22+where+time+%3E+now%28%29+-+24h+and+state%3D%27SUSPENDED%27+and+%28reason+%3C%3E+%27VERIFICATION_FAILED%27+and+reason+%3C%3E+%27POLL_FAILED%27%29+group+by+time%2815m%29+fill%280%29&u=admin HTTP/1.1 200 365 https://grafana.example.com/dashboard/db/fleet-view Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.80 Safari/537.36 377d9379-b273-11e5-bdcd-000000000000 892.850592ms
	tokens := strings.Split(line, " ")
	ntokens := len(tokens)
	var err error
	if !strings.HasPrefix(line, "[http]") {
		return nil, ErrNotHttp
	}
	if ntokens < 17 {
		return nil, ErrInvalidNumberOfTokens
	}
	var endTimestamp time.Time
	if endTimestamp, err = time.Parse(LOG_TIMESTAMP_FORMAT, tokens[1]+" "+tokens[2]); err != nil {
		return nil, err
	}

	var startTimestamp time.Time
	token := strings.Trim(tokens[6]+" "+tokens[7], "[]")
	if startTimestamp, err = time.Parse(LOG_TIMESTAMP_FORMAT_2, token); err != nil {
		return nil, err
	}

	ip := tokens[3]
	user := tokens[5]
	method := tokens[8]

	var u *url.URL
	if u, err = url.Parse(tokens[9]); err != nil {
		return nil, err
	}

	protocol := tokens[10]

	var status uint64
	if status, err = strconv.ParseUint(tokens[11], 10, 16); err != nil {
		return nil, err
	}
	var contentLength uint64
	if contentLength, err = strconv.ParseUint(tokens[12], 10, 32); err != nil {
		return nil, err
	}
	referrer := tokens[13]
	userAgent := strings.Join(tokens[14:ntokens-2], " ")

	requestId := tokens[ntokens-2]

	var duration time.Duration
	if duration, err = time.ParseDuration(tokens[ntokens-1]); err != nil {
		return nil, err
	}

	if duration < time.Millisecond {
		duration = time.Millisecond
	}

	// Adjust the start timestamp so that start and end times are distributed around the mid point
	// between whole seconds in a manner which preserves the accuracy of the originally reported timestamps.
	endTimestamp = endTimestamp.Round(time.Second).Add(time.Second - time.Millisecond)
	delta := endTimestamp.Sub(startTimestamp) - duration
	if delta > time.Millisecond*999 {
		delta = time.Millisecond * 999
	}
	if delta > 0 {
		startTimestamp = startTimestamp.Add(delta / 2)
	}

	return &decodedRecord{
		startTimestamp: startTimestamp,
		duration:       duration,
		method:         method,
		ip:             ip,
		user:           user,
		url:            urlToEncodedJSON(u),
		protocol:       protocol,
		status:         uint(status),
		contentLength:  uint32(contentLength),
		referrer:       referrer,
		userAgent:      userAgent,
		requestId:      requestId,
	}, nil
}

func (r *decodedRecord) Encode(c csv.Record) {
	c.Put(STARTED_AT, r.StartTimestamp().Format(TIMESTAMP_FORMAT))
	c.Put(DURATION, fmt.Sprintf("%d", uint(r.duration/time.Millisecond)))
	c.Put(IP, r.ip)
	c.Put(USER, r.user)
	c.Put(METHOD, r.method)
	c.Put(URL, r.url)
	c.Put(PROTOCOL, r.protocol)
	c.Put(STATUS, fmt.Sprintf("%d", r.status))
	c.Put(CONTENT_LENGTH, fmt.Sprintf("%d", r.contentLength))
	c.Put(REFERRER, r.referrer)
	c.Put(USER_AGENT, r.userAgent)
	c.Put(REQUEST_ID, r.requestId)
}
