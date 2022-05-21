package producerproxy

import (
	"github.com/Golang-Tools/optparams"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//Option 设置key行为的选项
type Options struct {
	kafka.ConfigMap
	ParallelCallback   bool
	NotConfirmDelivery bool
}

var DefaultOptions = Options{
	ConfigMap: kafka.ConfigMap{},
}

//WithParallelCallback 设置callback并行执行
func WithParallelCallback() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.ParallelCallback = true
	})
}

//WithoutConfirmDelivery 设置不监听确认发送成功与否
func WithoutConfirmDelivery() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		o.NotConfirmDelivery = true
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap["go.delivery.reports"] = false
	})
}

//AsBatchProducer 设置Producer为批发送模式,不推荐
func AsBatchProducer() optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap["go.batch.producer"] = true
	})
}

//WithAcks 设置发送的消息需要kafka中多少个成员确认
//params acks int kafka成员确认数量, 0表示不需要确认,-1表示需要全部成员确认,如果是其他正整数则不可以少于集群的最小成员数min.insync.replicas,默认为-1
func WithAcks(acks int) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap["acks"] = acks
	})
}

//WithQueueBufferingMaxDelay 设置发送消息时构建消息批传送给kafka之前等待生产者队列中的消息积累的延迟
//params delay int 构建消息批传送给kafka之前等待生产者队列中的消息积累的延迟,单位ms
func WithQueueBufferingMaxDelay(delay int) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap["queue.buffering.max.ms"] = delay
	})
}

//WithProducerSetting 设置监听时的其他设置,具体设置可以看<https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>
//和<https://github.com/confluentinc/confluent-kafka-go/blob/master/kafka/producer.go>中`NewProducer`的说明
//@params key string 设置项
//@params value any 设置项的值
func WithProducerSetting(key string, value any) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap[key] = value
	})
}
