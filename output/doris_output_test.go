package output

import (
	"fmt"
	"testing"
)

func TestDorisSend(t *testing.T) {

	var hosts []interface{} = make([]interface{}, 0)
	hosts = append(hosts, "http:127.0.0.1:8030")
	var config = map[interface{}]interface{}{"hosts": hosts}
	config["db"] = "ehome_db"
	config["table"] = "new_t_media"
	config["bulk_actions"] = 2
	config["flush_interval"] = 1
	config["worker"] = 1
	config["user"] = "doris"
	config["columns"] = "test,hh,kk"
	config["passwd"] = "doris123"
	var output = newDorisOutput(config)

	for i := 0; i < 4; i++ {
		msg := fmt.Sprintf("this is a test:%d", i)
		var event = map[string]interface{}{"test": msg}
		event["kk"] = nil
		event["hh"] = "mxq"
		event["uu"] = "sss"
		output.Emit(event)
	}

}
