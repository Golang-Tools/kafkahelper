package consumerproxy

import (
	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var Logger *log.Log

func init() {
	log.Set(log.WithExtFields(log.Dict{"module": "kafka-consumer-proxy"}))
	Logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
}

//Callback redis操作的回调函数
type Callback func(cli *kafka.Consumer) error

type OnMsgCallback func(evt *kafka.Message)
type OnErrorCallback func(err kafka.Error)

//ConsumerProxy redis客户端的代理
type ConsumerProxy struct {
	*kafka.Consumer
	Opt           Options
	callBacks     []Callback
	msgCallback   OnMsgCallback
	errorCallback OnErrorCallback
}

// New 创建一个新的数据库客户端代理
func New() *ConsumerProxy {
	proxy := new(ConsumerProxy)
	proxy.Opt = DefaultOptions
	proxy.callBacks = []Callback{}
	return proxy
}

// IsOk 检查代理是否已经可用
func (proxy *ConsumerProxy) IsOk() bool {
	return proxy.Consumer != nil
}

//SetConnect 设置连接的客户端
//@params cli UniversalClient 满足redis.UniversalClient接口的对象的指针
func (proxy *ConsumerProxy) SetConnect(cli *kafka.Consumer) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedClient
	}
	proxy.Consumer = cli
	if proxy.Opt.ParallelCallback {
		for _, cb := range proxy.callBacks {
			go func(cb Callback) {
				err := cb(proxy.Consumer)
				if err != nil {
					Logger.Error("regist callback get error", log.Dict{"err": err})
				} else {
					Logger.Debug("regist callback done")
				}
			}(cb)
		}
	} else {
		for _, cb := range proxy.callBacks {
			err := cb(proxy.Consumer)
			if err != nil {
				Logger.Error("regist callback get error", log.Dict{"err": err})
			} else {
				Logger.Debug("regist callback done")
			}
		}
	}
	return nil
}

//Init 从配置条件初始化代理对象
//@params endpoints string 设置etcd连接的地址端点,以`,`分隔
//@params opts ...optparams.Option[Options]
func (proxy *ConsumerProxy) Init(endpoints string, opts ...optparams.Option[Options]) error {
	optparams.GetOption(&proxy.Opt, opts...)
	if proxy.Opt.ConfigMap == nil {
		proxy.Opt.ConfigMap = kafka.ConfigMap{}
	}
	proxy.Opt.ConfigMap["bootstrap.servers"] = endpoints
	cli, err := kafka.NewConsumer(&proxy.Opt.ConfigMap)
	if err != nil {
		return err
	}
	return proxy.SetConnect(cli)
}

// Regist 注册回调函数,在init执行后执行回调函数
//如果对象已经设置了被代理客户端则无法再注册回调函数
//@params cb ...Callback 回调函数
func (proxy *ConsumerProxy) Regist(cb ...Callback) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedClient
	}
	proxy.callBacks = append(proxy.callBacks, cb...)
	return nil
}

//OnMessage 注册消息处理函数
//@params cb OnMsgCallback 消息处理的回调
func (proxy *ConsumerProxy) OnMessage(cb OnMsgCallback) error {
	if proxy.msgCallback != nil {
		return ErrProxyAllreadySettedCallback
	}
	proxy.msgCallback = cb
	return nil
}

//OnMessage 注册消息处理函数
//@params cb OnMsgCallback 消息处理的回调
func (proxy *ConsumerProxy) OnError(cb OnErrorCallback) error {
	if proxy.errorCallback != nil {
		return ErrProxyAllreadySettedCallback
	}
	proxy.errorCallback = cb
	return nil
}

//Watch 开始监听kafka
func (proxy *ConsumerProxy) Watch() func() {
	stopCh := make(chan struct{}, 1)
	go func() {
		run := true
		for run {
			select {
			case <-stopCh:
				Logger.Info("Stop Watching")
				run = false
			case ev := <-proxy.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					Logger.Error("Get AssignedPartitions event", log.Dict{"event": e})
					proxy.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					Logger.Error("Get RevokedPartitions event", log.Dict{"event": e})
					proxy.Unassign()
				case *kafka.Message:
					if proxy.msgCallback == nil {
						Logger.Info("Get Message", log.Dict{"topic": *e.TopicPartition.Topic, "key": string(e.Key), "value": string(e.Value)})
					} else {
						proxy.msgCallback(e)
					}
				case kafka.PartitionEOF:
					Logger.Info("Reached", log.Dict{"event": e})
				case kafka.Error:
					if proxy.errorCallback == nil {
						Logger.Error("Get error", log.Dict{"error": e})
					} else {
						proxy.errorCallback(e)
					}
				}
			}
		}
	}()
	return func() { close(stopCh) }
}

//Default 默认的kafka Consumer代理对象
var Default = New()
