package msghelper

import (
	"github.com/Golang-Tools/optparams"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//WithKey 设置key
//@params key []byte key的值
func WithKey(key []byte) optparams.Option[kafka.Message] {
	return optparams.NewFuncOption(func(o *kafka.Message) {
		o.Key = key
	})
}

//WithHeaders 设置headers
//@params headers map[string][]byte header键值对
func WithHeaders(headers map[string][]byte) optparams.Option[kafka.Message] {
	return optparams.NewFuncOption(func(o *kafka.Message) {
		if len(headers) > 0 {
			if len(o.Headers) > 0 {
				for k, v := range headers {
					o.Headers = append(o.Headers, kafka.Header{Key: k, Value: v})
				}
			} else {
				hs := []kafka.Header{}
				for k, v := range headers {
					hs = append(hs, kafka.Header{Key: k, Value: v})
				}
				o.Headers = hs
			}
		}
	})
}

//AddHeader 添加一个header
//@params key string header键
//@params value []byte header值
func AddHeader(key string, value []byte) optparams.Option[kafka.Message] {
	return optparams.NewFuncOption(func(o *kafka.Message) {
		if len(o.Headers) > 0 {
			o.Headers = append(o.Headers, kafka.Header{Key: key, Value: value})
		} else {
			o.Headers = []kafka.Header{{Key: key, Value: value}}
		}
	})
}

//WithPartition 设置发送去的Partition
//@params partition int32 分区号,默认为kafka.PartitionAny,即-1
func WithPartition(partition int32) optparams.Option[kafka.Message] {
	return optparams.NewFuncOption(func(o *kafka.Message) {
		o.TopicPartition.Partition = partition
	})
}
