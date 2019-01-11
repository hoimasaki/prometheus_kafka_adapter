//=============================================================================
//     FileName: main.go
//         Desc:
//       Author: Anakin Tu
//        Email: tuhui@bilibili.com
//     HomePage: https://github.com/sevnote
//      Version: 0.0.1
//   LastChange: 2019-01-11 11:33:41
//      History:
//=============================================================================
package main

import (
	//"fmt"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prompb"

	"github.com/confluent-kafka-go/kafka"
)

func main() {

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	//Kafka
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})

	defer p.Close()
	if err != nil {
		panic(err)
	}

	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()
	router.POST("/receive", func(c *gin.Context) {

		compressed, _ := ioutil.ReadAll(c.Request.Body)
		reqBuf, _ := snappy.Decode(nil, compressed)
		var req prompb.WriteRequest

		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			c.String(http.StatusBadRequest, "%s", err.Error())
			return
		}

		for _, ts := range req.Timeseries {
			m := make(model.Metric, len(ts.Labels))
			for _, l := range ts.Labels {
				m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
			}

			// Produce messages to topic (asynchronously)
			topic := "prometheus"

			p.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
				Value:          []byte(m.String()),
			}, nil)

			/*
				for _, s := range ts.Samples {
					fmt.Printf("  %f %d\n", s.Value, s.Timestamp)
				}
			*/
		}

		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	router.Run(":1234")
}
