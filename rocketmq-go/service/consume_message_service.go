/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package service

import (
	"sync"
	"time"

	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/config"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/constant"
	"github.com/golang/glog"
)

type ConsumeMessageService interface {
	//ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg, final String brokerName);

	Init(consumerGroup string, mqClient RocketMqClient, rebalance *Rebalance, offsetStore OffsetStore, defaultProducerService *DefaultProducerService, consumerConfig *config.RocketMqConsumerConfig)
	SubmitConsumeRequest(msgs []model.MessageExt, processQueue *model.ProcessQueue, messageQueue *model.MessageQueue, dispathToConsume bool)
	SendMessageBack(messageExt *model.MessageExt, delayLayLevel int, brokerName string) (err error)
	ConsumeMessageDirectly(messageExt *model.MessageExt, brokerName string) (consumeMessageDirectlyResult model.ConsumeMessageDirectlyResult, err error)
}

type ConsumeMessageConcurrentlyServiceImpl struct {
	consumerGroup                  string
	messageListener                model.MessageListener
	sendMessageBackProducerService SendMessageBackProducerService //for send retry Message
	offsetStore                    OffsetStore
	consumerConfig                 *config.RocketMqConsumerConfig
}

type ConsumeMessageOrderlyServiceImpl struct {
	consumerGroup   string
	messageListener model.MessageListenerOrderly
	offsetStore     OffsetStore
	consumerConfig  *config.RocketMqConsumerConfig //对应Java中的defaultMQPushConsumer
	mqLockTable     sync.Map
	rebalance       *Rebalance
}

func NewConsumeMessageConcurrentlyServiceImpl(messageListener model.MessageListener) (consumeService ConsumeMessageService) {
	consumeService = &ConsumeMessageConcurrentlyServiceImpl{messageListener: messageListener, sendMessageBackProducerService: &SendMessageBackProducerServiceImpl{}}
	return
}

//
func NewConsumeMessageOrderlyServiceImpl(messageListener model.MessageListenerOrderly) (consumeService ConsumeMessageService) {
	consumeService = &ConsumeMessageOrderlyServiceImpl{messageListener: messageListener}
	return
}

func (self *ConsumeMessageConcurrentlyServiceImpl) Init(consumerGroup string, mqClient RocketMqClient, rebalance *Rebalance, offsetStore OffsetStore, defaultProducerService *DefaultProducerService, consumerConfig *config.RocketMqConsumerConfig) {
	self.consumerGroup = consumerGroup
	self.offsetStore = offsetStore
	self.sendMessageBackProducerService.InitSendMessageBackProducerService(consumerGroup, mqClient, defaultProducerService, consumerConfig)
	self.consumerConfig = consumerConfig
}

//
func (self *ConsumeMessageOrderlyServiceImpl) Init(consumerGroup string, mqClient RocketMqClient, rebalance *Rebalance, offsetStore OffsetStore, defaultProducerService *DefaultProducerService, consumerConfig *config.RocketMqConsumerConfig) {
	self.consumerGroup = consumerGroup
	self.rebalance = rebalance
	self.offsetStore = offsetStore
	self.consumerConfig = consumerConfig
}

func (self *ConsumeMessageConcurrentlyServiceImpl) SubmitConsumeRequest(msgs []model.MessageExt, processQueue *model.ProcessQueue, messageQueue *model.MessageQueue, dispatchToConsume bool) {
	msgsLen := len(msgs)
	for i := 0; i < msgsLen; {
		begin := i
		end := i + self.consumerConfig.ConsumeMessageBatchMaxSize
		if end > msgsLen {
			end = msgsLen
		}
		go func() {
			glog.V(2).Infof("look slice begin %d end %d msgsLen %d", begin, end, msgsLen)
			batchMsgs := transformMessageToConsume(self.consumerGroup, msgs[begin:end])
			consumeState := self.messageListener(batchMsgs)
			self.processConsumeResult(consumeState, batchMsgs, messageQueue, processQueue)
		}()
		i = end
	}
	return
}

func (self *ConsumeMessageOrderlyServiceImpl) SubmitConsumeRequest(msg []model.MessageExt, processQueue *model.ProcessQueue, messageQueue *model.MessageQueue, dispatchToConsume bool) {
	if dispatchToConsume {
		go func() {
			if processQueue.IsDropped() {
				glog.Warningf("the message queue %+v not be able to consume, because it's dropped.", *messageQueue)
				return
			}

			mqLock := self.fetchMessageQueueLock(*messageQueue)
			defer mqLock.Unlock()
			mqLock.Lock()
			if processQueue.IsLocked() && !processQueue.IsLockExpired() {
				beginTime := time.Now()
				for continueConsume := true; continueConsume; {
					if processQueue.IsDropped() {
						glog.Warningf("the message queue not be able to consume, because it's dropped. %+v", *messageQueue)
						break
					}
					if !processQueue.IsLocked() {
						glog.Warningf("the message queue not locked, so consume later, %+v", *messageQueue)
						self.tryLockLaterAndReconsume(messageQueue, processQueue, 10*time.Millisecond)
						break
					}
					if processQueue.IsLockExpired() {
						glog.Warningf("the message queue lock expired, so consume later, %+v", *messageQueue)
						self.tryLockLaterAndReconsume(messageQueue, processQueue, 10*time.Millisecond)
						break
					}
					interval := time.Now().Sub(beginTime)
					if interval > constant.MAX_TIME_CONSUME_CONTINUOUSLY {
						glog.Warningf("xx")
						self.submitConsumeRequestLater(processQueue, messageQueue, 10*time.Millisecond)
						break
					}

					batchMsgs := processQueue.TakeMessages(self.consumerConfig.ConsumeMessageBatchMaxSize)
					if len(batchMsgs) > 0 {
						consumeState := self.messageListener(batchMsgs)
						// defer processQueue.GetLockConsume().Unlock()
						// processQueue.GetLockConsume().Lock()
						if processQueue.IsDropped() {
							break
						}
						continueConsume = self.processConsumeResult(consumeState, batchMsgs, messageQueue, processQueue)
					} else {
						continueConsume = false
					}
				}
			} else {
				if processQueue.IsDropped() {
					glog.Warningf("the message queue not be able to consume, because it's dropped. %+v", *messageQueue)
					return
				}
				glog.Warning("Fail to acquire locks from broker, try to lock later and reconsume")
				self.tryLockLaterAndReconsume(messageQueue, processQueue, 100*time.Millisecond)
			}
		}()
	}
}

func (self *ConsumeMessageOrderlyServiceImpl) submitConsumeRequestLater(processQueue *model.ProcessQueue, messageQueue *model.MessageQueue, delay time.Duration) {
	go func() {
		<-time.After(delay)
		self.SubmitConsumeRequest(nil, processQueue, messageQueue, true)
	}()
}

func (self *ConsumeMessageConcurrentlyServiceImpl) SendMessageBack(messageExt *model.MessageExt, delayLayLevel int, brokerName string) (err error) {
	err = self.sendMessageBackProducerService.SendMessageBack(messageExt, 0, brokerName)
	return
}

func (self *ConsumeMessageOrderlyServiceImpl) SendMessageBack(messageExt *model.MessageExt, delayLayLevel int, brokerName string) error {
	return nil
}

func (self *ConsumeMessageConcurrentlyServiceImpl) ConsumeMessageDirectly(messageExt *model.MessageExt, brokerName string) (consumeMessageDirectlyResult model.ConsumeMessageDirectlyResult, err error) {
	start := time.Now().UnixNano() / 1000000
	consumeResult := self.messageListener([]model.MessageExt{*messageExt})
	consumeMessageDirectlyResult.AutoCommit = true
	consumeMessageDirectlyResult.Order = false
	consumeMessageDirectlyResult.SpentTimeMills = time.Now().UnixNano()/1000000 - start
	if consumeResult.ConsumeConcurrentlyStatus == "CONSUME_SUCCESS" && consumeResult.AckIndex >= 0 {
		consumeMessageDirectlyResult.ConsumeResult = "CR_SUCCESS"

	} else {
		consumeMessageDirectlyResult.ConsumeResult = "CR_THROW_EXCEPTION"
	}
	return
}

func (self *ConsumeMessageOrderlyServiceImpl) ConsumeMessageDirectly(messageExt *model.MessageExt, brokerName string) (consumeMessageDirectlyResult model.ConsumeMessageDirectlyResult, err error) {
	start := time.Now().UnixNano() / 1000000
	consumeResult := self.messageListener([]model.MessageExt{*messageExt})
	consumeMessageDirectlyResult.AutoCommit = true
	consumeMessageDirectlyResult.Order = true
	consumeMessageDirectlyResult.SpentTimeMills = time.Now().UnixNano()/1000000 - start
	if consumeResult.ConsumeOrderlyStatus == "SUCCESS" {
		consumeMessageDirectlyResult.ConsumeResult = "CR_SUCCESS"
	} else {
		consumeMessageDirectlyResult.ConsumeResult = "CR_LATER"
	}
	return
}

func (self *ConsumeMessageConcurrentlyServiceImpl) processConsumeResult(result model.ConsumeConcurrentlyResult, msgs []model.MessageExt, messageQueue *model.MessageQueue, processQueue *model.ProcessQueue) {
	if processQueue.IsDropped() {
		glog.Warning("processQueue is dropped without process consume result. ", msgs)
		return
	}
	if len(msgs) == 0 {
		return
	}
	ackIndex := result.AckIndex
	if model.CONSUME_SUCCESS == result.ConsumeConcurrentlyStatus {
		if ackIndex >= len(msgs) {
			ackIndex = len(msgs) - 1
		} else {
			if result.AckIndex < 0 {
				ackIndex = -1
			}
		}
	}
	var failedMessages []model.MessageExt
	successMessages := []model.MessageExt{}
	if ackIndex >= 0 {
		successMessages = msgs[:ackIndex+1]
	}
	for i := ackIndex + 1; i < len(msgs); i++ {
		err := self.SendMessageBack(&msgs[i], 0, messageQueue.BrokerName)
		if err != nil {
			msgs[i].ReconsumeTimes = msgs[i].ReconsumeTimes + 1
			failedMessages = append(failedMessages, msgs[i])
		} else {
			successMessages = append(successMessages, msgs[i])
		}
	}
	if len(failedMessages) > 0 {
		self.SubmitConsumeRequest(failedMessages, processQueue, messageQueue, true)
	}
	commitOffset := processQueue.RemoveMessage(successMessages)
	if commitOffset > 0 && !processQueue.IsDropped() {
		self.offsetStore.UpdateOffset(messageQueue, commitOffset, true)
	}

}

func (self *ConsumeMessageOrderlyServiceImpl) processConsumeResult(result model.ConsumeOrderlyResult, msgs []model.MessageExt, messageQueue *model.MessageQueue, processQueue *model.ProcessQueue) bool {
	continueConsume := true
	var commitOffset int64 = -1
	if result.ConsumeOrderlyStatus == model.SUSPEND_CURRENT_QUEUE_A_MOMENT {
		processQueue.MakeMessagesConsumeAgain(msgs)
		self.submitConsumeRequestLater(processQueue, messageQueue, 10*time.Millisecond)
		continueConsume = false
	} else {
		// msgs消费成功
		// msgs是按照QueueOffset排序的数组，最后一个元素offset最大
		commitOffset = processQueue.Commit(msgs)
	}
	if commitOffset >= 0 && !processQueue.IsDropped() {
		self.offsetStore.UpdateOffset(messageQueue, commitOffset, false)
	}
	return continueConsume
}

func transformMessageToConsume(consumerGroup string, msgs []model.MessageExt) []model.MessageExt {
	retryTopicName := constant.RETRY_GROUP_TOPIC_PREFIX + consumerGroup

	for _, msg := range msgs {
		//reset retry topic name
		if msg.Message.Topic == retryTopicName {
			retryTopic := msg.Properties[constant.PROPERTY_RETRY_TOPIC]
			if len(retryTopic) > 0 {
				msg.Message.Topic = retryTopic
			}
		}
		//set consume start time
		msg.SetConsumeStartTime()
	}
	return msgs
}

func (self *ConsumeMessageOrderlyServiceImpl) fetchMessageQueueLock(mq model.MessageQueue) *sync.Mutex {
	val, ok := self.mqLockTable.Load(mq)
	if ok {
		return val.(*sync.Mutex)
	}
	lock := new(sync.Mutex)
	val, loaded := self.mqLockTable.LoadOrStore(mq, lock)
	if loaded {
		lock = val.(*sync.Mutex)
	}
	return lock
}

func (self *ConsumeMessageOrderlyServiceImpl) tryLockLaterAndReconsume(messageQueue *model.MessageQueue, processQueue *model.ProcessQueue, delay time.Duration) {
	go func() {
		<-time.After(delay)
		lockOK := self.rebalance.Lock(messageQueue)
		if lockOK {
			self.submitConsumeRequestLater(processQueue, messageQueue, 10*time.Millisecond)
		} else {
			self.submitConsumeRequestLater(processQueue, messageQueue, 3*time.Second)
		}
	}()
}
