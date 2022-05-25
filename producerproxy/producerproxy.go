package producerproxy

import (
	log "github.com/Golang-Tools/loggerhelper/v2"
	"github.com/Golang-Tools/optparams"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var Logger *log.Log

func init() {
	log.Set(log.WithExtFields(log.Dict{"module": "kafka-producer-proxy"}))
	Logger = log.Export()
	log.Set(log.WithExtFields(log.Dict{}))
}

//SetConnectCallback
type SetConnectCallback func(cli *kafka.Producer) error

type DeliveryCallback func(evt *kafka.Message)
type DeliveryUnknownEventCallback func(evt kafka.Event)

//ProducerProxy redis客户端的代理
type ProducerProxy struct {
	*kafka.Producer
	Opt                          Options
	delivered_records            int64
	deliver_watching             bool
	callBacks                    []SetConnectCallback
	deliveryCallback             []DeliveryCallback
	deliveryErrorCallback        []DeliveryCallback
	deliveryIgnoredEventCallback []DeliveryUnknownEventCallback
}

//New 创建一个新的kafka Producer客户端代理
func New() *ProducerProxy {
	proxy := new(ProducerProxy)
	proxy.Opt = DefaultOptions
	proxy.callBacks = []SetConnectCallback{}
	proxy.delivered_records = 0
	proxy.deliver_watching = false
	return proxy
}

//IsOk 检查代理是否已经可用
func (proxy *ProducerProxy) IsOk() bool {
	return proxy.Producer != nil
}

//IsWatchingDeliver( 检查代理是否正在监听发送情况
func (proxy *ProducerProxy) IsWatchingDeliver() bool {
	return proxy.deliver_watching
}

//DeliveredRecords 查看已经发送了几条信息
func (proxy *ProducerProxy) DeliveredRecords() int64 {
	return proxy.delivered_records
}

//Close 关闭发送端
func (proxy *ProducerProxy) Close() {
	proxy.Producer.Close()
	if proxy.IsWatchingDeliver() {
		proxy.deliver_watching = false
	}
}

//SetConnect 设置连接的客户端
//@params cli *kafka.Producer 被代理
func (proxy *ProducerProxy) SetConnect(cli *kafka.Producer) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedClient
	}
	proxy.Producer = cli
	if proxy.Opt.ParallelCallback {
		for _, cb := range proxy.callBacks {
			go func(cb SetConnectCallback) {
				err := cb(proxy.Producer)
				if err != nil {
					Logger.Error("regist callback get error", log.Dict{"err": err})
				} else {
					Logger.Debug("regist callback done")
				}
			}(cb)
		}
	} else {
		for _, cb := range proxy.callBacks {
			err := cb(proxy.Producer)
			if err != nil {
				Logger.Error("regist callback get error", log.Dict{"err": err})
			} else {
				Logger.Debug("regist callback done")
			}
		}
	}

	if !proxy.Opt.NotConfirmDelivery {
		proxy.StartConfirmDelivery()
		proxy.deliver_watching = true
	}
	return nil
}

//Init 从配置条件初始化代理对象
//@params endpoints string 设置etcd连接的地址端点,以`,`分隔
//@params opts ...optparams.Option[Options]
func (proxy *ProducerProxy) Init(endpoints string, opts ...optparams.Option[Options]) error {
	optparams.GetOption(&proxy.Opt, opts...)
	if proxy.Opt.ConfigMap == nil {
		proxy.Opt.ConfigMap = kafka.ConfigMap{}
	}
	proxy.Opt.ConfigMap["bootstrap.servers"] = endpoints
	cli, err := kafka.NewProducer(&proxy.Opt.ConfigMap)
	if err != nil {
		return err
	}
	return proxy.SetConnect(cli)
}

// Regist 注册回调函数,在init执行后执行回调函数
//如果对象已经设置了被代理客户端则无法再注册回调函数
//@params cb ...Callback 回调函数
func (proxy *ProducerProxy) Regist(cb ...SetConnectCallback) error {
	if proxy.IsOk() {
		return ErrProxyAllreadySettedClient
	}
	proxy.callBacks = append(proxy.callBacks, cb...)
	return nil
}

//OnDelivery 注册当发未设置WithoutConfirmDelivery且发送成功时执行的回调
//params cb ...DeliveryCallback 当发未设置WithoutConfirmDelivery且发送成功时执行的回调
func (proxy *ProducerProxy) OnDelivery(cb ...DeliveryCallback) error {
	if proxy.IsWatchingDeliver() {
		return ErrDeliverIsWatching
	}
	proxy.deliveryCallback = append(proxy.deliveryCallback, cb...)
	return nil
}

//OnDeliveryError 注册当发未设置WithoutConfirmDelivery且发送失败时执行的回调
//params cb ...DeliveryCallback 当发未设置WithoutConfirmDelivery且发送失败时执行的回调
func (proxy *ProducerProxy) OnDeliveryError(cb ...DeliveryCallback) error {
	if proxy.IsWatchingDeliver() {
		return ErrDeliverIsWatching
	}
	proxy.deliveryErrorCallback = append(proxy.deliveryErrorCallback, cb...)
	return nil
}

//OnDeliveryUnknownEventCallback 注册当发未设置WithoutConfirmDelivery且监听到未知类型事件时的回调
//params cb ...DeliveryUnknownEventCallback 当发未设置WithoutConfirmDelivery且监听到未知类型事件时的回调
func (proxy *ProducerProxy) OnDeliveryUnknownEventCallback(cb ...DeliveryUnknownEventCallback) error {
	if proxy.IsWatchingDeliver() {
		return ErrDeliverIsWatching
	}
	proxy.deliveryIgnoredEventCallback = append(proxy.deliveryIgnoredEventCallback, cb...)
	return nil
}

//StartConfirmDelivery 启动发送消息验收,用于确认发送成功
func (proxy *ProducerProxy) StartConfirmDelivery() error {
	if !proxy.IsOk() {
		return ErrProxyNotYetSettedClient
	}
	func() {
		for e := range proxy.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				{
					if ev.TopicPartition.Error != nil {
						if len(proxy.deliveryErrorCallback) > 0 {
							for _, cb := range proxy.deliveryErrorCallback {
								cb(ev)
							}
						} else {
							Logger.Error("Delivery failed", log.Dict{"TopicPartition": ev.TopicPartition})
						}
					} else {
						proxy.delivered_records += 1
						if len(proxy.deliveryCallback) > 0 {
							for _, cb := range proxy.deliveryCallback {
								cb(ev)
							}
						} else {
							Logger.Info("Delivered message", log.Dict{"TopicPartition": ev.TopicPartition})
						}
					}
				}
			default:
				{
					if len(proxy.deliveryIgnoredEventCallback) > 0 {
						for _, cb := range proxy.deliveryIgnoredEventCallback {
							cb(e)
						}
					} else {
						Logger.Error("kafka producer Ignored event", log.Dict{"ev": ev})
					}
				}
			}
		}
	}()
	return nil
}

func (proxy *ProducerProxy) sendAsync(msg *kafka.Message) {
	proxy.ProduceChannel() <- msg
}

func (proxy *ProducerProxy) Send(msg *kafka.Message) {
	go proxy.sendAsync(msg)
}

func (proxy *ProducerProxy) SendAndWait(msg *kafka.Message) error {
	err := proxy.Produce(msg, nil)
	return err
}

//Default 默认的kafka Producer代理对象
var Default = New()
