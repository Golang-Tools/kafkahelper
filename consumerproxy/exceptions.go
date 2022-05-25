package consumerproxy

import "errors"

//ErrProxyAllreadySettedClient 代理已经设置过kafka消费者客户端
var ErrProxyAllreadySettedClient = errors.New("cannot reset consumer")

//ErrProxyAllreadySettedCallback 代理已经设置过回调函数
var ErrProxyAllreadySettedCallback = errors.New("cannot reset callback")
