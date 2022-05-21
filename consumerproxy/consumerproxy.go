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

//ConsumerProxy redis客户端的代理
type ConsumerProxy struct {
	*kafka.Consumer
	Opt       Options
	callBacks []Callback
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

//Default 默认的kafka Consumer代理对象
var Default = New()
