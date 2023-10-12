package output

import (
	"encoding/base64"
	"fmt"
	"github.com/childe/gohangout/codec"
	"github.com/childe/gohangout/topology"
	"github.com/golang/glog"
	"net/http"
	"strings"
	"time"
)

type DorisAction struct {
	event map[string]interface{}
}

func (action *DorisAction) Encode() []byte {
	var (
		buf []byte
		err error
	)
	buf, err = f().Encode(action.event)
	if err != nil {
		glog.Errorf("could marshal event(%v):%s", action.event, err)
		return nil
	}

	bulk_buf := make([]byte, 0, len(buf)+1)
	bulk_buf = append(bulk_buf, buf[:]...)
	bulk_buf = append(bulk_buf, '\n')
	return bulk_buf
}

type DorisBulkRequest struct {
	bulk_buf []byte
	retry    int
	len      int
}

func (br *DorisBulkRequest) retryTimes() int {
	return br.retry
}

func (br *DorisBulkRequest) retryRequest() {
	br.retry = br.retry + 1
}

func (br *DorisBulkRequest) add(event Event) {
	br.bulk_buf = append(br.bulk_buf, event.Encode()...)
	br.len++
}

func (br *DorisBulkRequest) bufSizeByte() int {
	return len(br.bulk_buf)
}
func (br *DorisBulkRequest) eventCount() int {
	return br.len
}
func (br *DorisBulkRequest) readBuf() []byte {
	return br.bulk_buf
}

type DorisOutput struct {
	config map[interface{}]interface{}

	bulkProcessor BulkProcessor
	db            string
	table         string
	user          string
	password      string
	hosts         []string
}

func dorisGetRetryEvents(resp *http.Response, respBody []byte, bulkRequest *BulkRequest) ([]int, []int, BulkRequest) {
	retry := make([]int, 0)
	noRetry := make([]int, 0)
	//make a string index to avoid json decode for speed up over 90%+ scences
	return retry, noRetry, nil
}

func init() {
	Register("doris", newDorisOutput)
}

type CsvEncoder struct {
	delimiter    string
	fields       []string
	nestedFields [][]string
}

func (e *CsvEncoder) Encode(v interface{}) ([]byte, error) {
	event := v.(map[string]interface{})
	array := make([]string, 0)
	for _, f := range e.fields {
		val := event[f]
		if val != nil {
			array = append(array, val.(string))
		} else {
			array = append(array, "-")
		}
	}

	for _, f := range e.nestedFields {
		array = e.loopMap(event, array, f, 0)
	}

	str := strings.Join(array, e.delimiter)
	return []byte(str), nil
}

func (e *CsvEncoder) loopMap(event map[string]interface{}, ar []string, field []string, index int) []string {

	key := field[index]
	value := event[key]
	if value != nil {
		cmap, ok := value.(map[string]interface{})
		if ok {
			return e.loopMap(cmap, ar, field, index+1)
		} else {
			ar = append(ar, value.(string))
			return ar
		}
	} else {
		ar = append(ar, "-")
		return ar
	}

}

func newDorisOutput(config map[interface{}]interface{}) topology.Output {

	columns := make([]string, 0)
	if v, ok := config["columns"]; ok {
		var str = v.(string)
		tmp := strings.Split(str, ",")
		for _, s := range tmp {
			columns = append(columns, s)
		}

	} else {
		glog.Fatal("must config columns")
	}

	nestedFields := make([][]string, 0)
	columStr := config["columns"].(string)
	if v, ok := config["nested_field"]; ok {
		var str = v.(string)
		tmp := strings.Split(str, ",")
		for _, s := range tmp {
			array := strings.Split(s, ".")
			nestedFields = append(nestedFields, array)
			f := strings.Join(array, "_")
			columStr = columStr + "," + f
		}

	} else {
		glog.Fatal("must config columns")
	}

	var del = "\x01"

	f = func() codec.Encoder { return &CsvEncoder{delimiter: del, fields: columns, nestedFields: nestedFields} }

	var (
		bulk_size, bulk_actions, flush_interval, concurrent int
		compress                                            bool
		db, table, user, passwd                             string
	)
	if v, ok := config["bulk_size"]; ok {
		bulk_size = v.(int) * 1024 * 1024
	} else {
		bulk_size = DEFAULT_BULK_SIZE
	}

	if v, ok := config["bulk_actions"]; ok {
		bulk_actions = v.(int)
	} else {
		bulk_actions = DEFAULT_BULK_ACTIONS
	}
	if v, ok := config["flush_interval"]; ok {
		flush_interval = v.(int)
	} else {
		flush_interval = DEFAULT_FLUSH_INTERVAL
	}
	if v, ok := config["concurrent"]; ok {
		concurrent = v.(int)
	} else {
		concurrent = DEFAULT_CONCURRENT
	}
	if concurrent <= 0 {
		glog.Fatal("concurrent must > 0")
	}
	if v, ok := config["compress"]; ok {
		compress = v.(bool)
	} else {
		compress = true
	}

	if v, ok := config["db"]; ok {
		db = v.(string)
	} else {
		glog.Fatal("must config db")
	}

	if v, ok := config["table"]; ok {
		table = v.(string)
	} else {
		glog.Fatal("must config table")
	}

	if v, ok := config["user"]; ok {
		user = v.(string)
	} else {
		glog.Fatal("must config user")
	}

	if v, ok := config["passwd"]; ok {
		passwd = v.(string)
	} else {
		glog.Fatal("must config passwd")
	}

	rst := &DorisOutput{
		config: config,
	}

	var headers = map[string]string{"format": "json"}
	if v, ok := config["headers"]; ok {
		for keyI, valueI := range v.(map[interface{}]interface{}) {
			headers[keyI.(string)] = valueI.(string)
		}
	}

	up := fmt.Sprintf("%s:%s", user, passwd)
	headers["format"] = "csv"
	headers["strict_mode"] = "false"
	headers["max_filter_ratio"] = "0.01"
	headers["Expect"] = "100-continue"
	headers["columns"] = columStr
	headers["column_separator"] = "\\x01"
	headers["compress_type"] = "gz"
	headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(up))
	var requestMethod string = "PUT"
	var retryResponseCode map[int]bool = make(map[int]bool)
	if v, ok := config["retry_response_code"]; ok {
		for _, cI := range v.([]interface{}) {
			retryResponseCode[cI.(int)] = true
		}
	} else {
		retryResponseCode[401] = true
		retryResponseCode[502] = true
	}

	byte_size_applied_in_advance := bulk_size + 1024*1024
	if byte_size_applied_in_advance > MAX_BYTE_SIZE_APPLIED_IN_ADVANCE {
		byte_size_applied_in_advance = MAX_BYTE_SIZE_APPLIED_IN_ADVANCE
	}
	var f = func() BulkRequest {
		return &DorisBulkRequest{
			bulk_buf: make([]byte, 0, byte_size_applied_in_advance),
		}
	}

	var hosts []string = make([]string, 0)
	if v, ok := config["hosts"]; ok {
		for _, h := range v.([]interface{}) {
			hosts = append(hosts, h.(string))
		}
	} else {
		glog.Fatal("hosts must be set in elasticsearch output")
	}
	rst.hosts = hosts

	url := fmt.Sprintf("/api/%s/%s/_stream_load", db, table)
	rst.bulkProcessor = NewHTTPBulkProcessor(headers, hosts, requestMethod, retryResponseCode, bulk_size, bulk_actions, flush_interval, concurrent, compress, f, dorisGetRetryEvents, url)
	return rst
}

// Emit adds the event to bulkProcessor
func (p *DorisOutput) Emit(event map[string]interface{}) {
	p.bulkProcessor.add(&DorisAction{event})
}

func (outputPlugin *DorisOutput) Shutdown() {
	outputPlugin.bulkProcessor.awaitclose(30 * time.Second)
}
