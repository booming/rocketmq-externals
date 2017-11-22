package rocketmq

import (
	"time"
)

// 默认每20秒向broker发送一次加锁请求
const DEFAULT_REBALANCE_LOCK_INTERVAL time.Duration = 20 * time.Second

type LockMQController struct {
	clientFactory *ClientFactory
}

func NewLockMQController(factory *ClientFactory) *LockMQController {
	return &LockMQController{clientFactory: factory}
}

func (self *LockMQController) Start() {
	for _, consumer := range self.clientFactory.ConsumerTable {
		if consumer.IsConsumeOrderly() {
			go func() {
				timer := time.NewTimer(DEFAULT_REBALANCE_LOCK_INTERVAL)
				for {
					<-timer.C
					consumer.rebalance.LockAll()
					timer.Reset(DEFAULT_REBALANCE_LOCK_INTERVAL)
				}
			}()
		}
	}
}
