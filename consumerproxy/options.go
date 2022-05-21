package consumerproxy

import (
	"bytes"
	"strings"

	"github.com/Golang-Tools/optparams"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

//Options 设置代理对象初始化方法的可选参数
type Options struct {
	kafka.ConfigMap
	ParallelCallback bool
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

//WithGroupID 设置监听时使用的groupid
//@params groupID string 指定的groupid
func WithGroupID(groupID string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap["group.id"] = groupID
	})
}

//WithGroupID 设置监听时使用uuid4作为groupid
//@params namespace ...string 指定的groupid的命名空间,命名空间会按顺序使用`-`连接,然后作为前缀与uuid使用`__`相连
func WithUUID4GroupID(namespace ...string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		u4 := uuid.New()
		uuid_str := u4.String()
		groupid := uuid_str
		if len(namespace) > 0 {
			prefix := strings.Join(namespace, "-")
			buffer := bytes.Buffer{}
			buffer.WriteString(prefix)
			buffer.WriteString("__")
			buffer.WriteString(uuid_str)
			groupid = buffer.String()
		}
		o.ConfigMap["group.id"] = groupid
	})
}

//WithAutoOffsetReset 设置监听时自动重置offset的策略
//@params strategy string 自动重置offset的策略,可选的有earliest,latest,err3种
func WithAutoOffsetReset(strategy string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap["auto.offset.reset"] = strategy
	})
}

//WithIsolationLevel 设置监听时读取消息的策略
//@params strategy string 读取消息的策略,可选的有read_uncommitted, read_committed2种
func WithIsolationLevel(strategy string) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap["isolation.level"] = strategy
	})
}

//WithComsumerSetting 设置监听时的其他设置,具体设置可以看<https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>
//@params key string 设置项
//@params value any 设置项的值
func WithComsumerSetting(key string, value any) optparams.Option[Options] {
	return optparams.NewFuncOption(func(o *Options) {
		if o.ConfigMap == nil {
			o.ConfigMap = kafka.ConfigMap{}
		}
		o.ConfigMap[key] = value
	})
}
