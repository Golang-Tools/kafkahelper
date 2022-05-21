package producerproxy

import "errors"

//ErrProxyAllreadySettedClient 代理已经设置过redis客户端对象
var ErrProxyAllreadySettedClient = errors.New("cannot reset producer")

//ErrProxyNotYetSettedClient 代理还未设置客户端对象
var ErrProxyNotYetSettedClient = errors.New("not set producer yet")

//ErrDeliverIsWatching 代理还未设置客户端对象
var ErrDeliverIsWatching = errors.New("can not set callback when deliver is watching")
