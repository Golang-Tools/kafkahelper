package consumerproxy

import "errors"

//ErrProxyAllreadySettedClient 代理已经设置过redis客户端对象
var ErrProxyAllreadySettedClient = errors.New("cannot reset consumer")
