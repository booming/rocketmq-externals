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
	"errors"
	"fmt"
	"time"

	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/config"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/constant"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/header"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/remoting"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/util"
	"github.com/golang/glog"
)

type MessageQueueSelector func([]model.MessageQueue, *model.Message, interface{}) (model.MessageQueue, error)

type ProducerService interface {
	CheckConfig() (err error)
	SendDefaultImpl(message *model.Message, communicationMode string, sendCallback string, timeout int64) (sendResult *model.SendResult, err error)
	SendSelectImpl(message *model.Message, selector MessageQueueSelector, arg interface{}, communicationMode string, sendCallback string, timeout int64) (sendResult *model.SendResult, err error)
	SendQueueImpl(message *model.Message, queue model.MessageQueue, communicationMode string, sendCallback string, timeout int64) (sendResult *model.SendResult, err error)
}

type DefaultProducerService struct {
	producerGroup   string
	producerConfig  *config.RocketMqProducerConfig
	mqClient        RocketMqClient
	mqFaultStrategy MQFaultStrategy
}

func NewDefaultProducerService(producerGroup string, producerConfig *config.RocketMqProducerConfig, mqClient RocketMqClient) (defaultProducerService *DefaultProducerService) {
	defaultProducerService = &DefaultProducerService{
		mqClient:       mqClient,
		producerGroup:  producerGroup,
		producerConfig: producerConfig,
	}
	defaultProducerService.CheckConfig()
	return
}
func (self *DefaultProducerService) CheckConfig() (err error) {
	// todo check if not pass panic
	return
}

func (self *DefaultProducerService) SendDefaultImpl(message *model.Message, communicationMode string, sendCallback string, timeout int64) (sendResult *model.SendResult, err error) {
	var (
		topicPublishInfo *model.TopicPublishInfo
	)
	err = self.checkMessage(message)
	if err != nil {
		return
	}
	topicPublishInfo, err = self.mqClient.TryToFindTopicPublishInfo(message.Topic)
	if err != nil {
		return
	}
	if topicPublishInfo.JudgeTopicPublishInfoOk() == false {
		err = errors.New("topicPublishInfo is error,topic=" + message.Topic)
		return
	}
	glog.V(2).Info("op=look topicPublishInfo", topicPublishInfo)
	//if(!ok) return error
	sendResult, err = self.sendMsgUseTopicPublishInfo(message, communicationMode, sendCallback, topicPublishInfo, timeout)
	return
}

func (self *DefaultProducerService) SendSelectImpl(message *model.Message, selector MessageQueueSelector, arg interface{}, communicationMode string, sendCallback string, timeout int64) (sendResult *model.SendResult, err error) {
	var (
		topicPublishInfo *model.TopicPublishInfo
	)
	err = self.checkMessage(message)
	if err != nil {
		return
	}
	topicPublishInfo, err = self.mqClient.TryToFindTopicPublishInfo(message.Topic)
	if err != nil {
		return
	}
	if topicPublishInfo.JudgeTopicPublishInfoOk() == false {
		err = errors.New("topicPublishInfo is error,topic=" + message.Topic)
		return
	}
	glog.V(2).Info("op=look topicPublishInfo", topicPublishInfo)

	messageQueue, err := selector(topicPublishInfo.MessageQueueList, message, arg)
	if err != nil {
		return
	}

	//TODO add retry
	sendResult, err = self.doSendMessage(message, messageQueue, communicationMode, sendCallback, topicPublishInfo, timeout)
	return
}

func (self *DefaultProducerService) SendQueueImpl(message *model.Message, queue model.MessageQueue, communicationMode string, sendCallback string, timeout int64) (sendResult *model.SendResult, err error) {
	err = self.checkMessage(message)
	if err != nil {
		return
	}
	if message.Topic != queue.Topic {
		err = fmt.Errorf("message's topic not equal queue's topic")
		return
	}

	// TODO add retry
	sendResult, err = self.doSendMessage(message, queue, communicationMode, sendCallback, nil, timeout)
	return
}

func (self *DefaultProducerService) producerSendMessageRequest(brokerAddr string, sendMessageHeader remoting.CustomerHeader, message *model.Message, timeout int64) (sendResult *model.SendResult, err error) {
	remotingCommand := remoting.NewRemotingCommandWithBody(remoting.SEND_MESSAGE, sendMessageHeader, message.Body)
	var response *remoting.RemotingCommand
	response, err = self.mqClient.GetRemotingClient().InvokeSync(brokerAddr, remotingCommand, timeout)
	if err != nil {
		glog.Error(err)
		return
	}
	sendResult, err = processSendResponse(brokerAddr, message, response)
	return
}
func processSendResponse(brokerName string, message *model.Message, response *remoting.RemotingCommand) (sendResult *model.SendResult, err error) {
	sendResult = &model.SendResult{}
	switch response.Code {
	case remoting.FLUSH_DISK_TIMEOUT:
		{
			sendResult.SetSendStatus(model.FlushDiskTimeout)
			break
		}
	case remoting.FLUSH_SLAVE_TIMEOUT:
		{
			sendResult.SetSendStatus(model.FlushSlaveTimeout)
			break
		}
	case remoting.SLAVE_NOT_AVAILABLE:
		{
			sendResult.SetSendStatus(model.SlaveNotAvaliable)
			break
		}
	case remoting.SUCCESS:
		{
			sendResult.SetSendStatus(model.SendOK)
			break
		}
	default:
		err = errors.New("response.Code error code")
		return
	}
	var responseHeader = &header.SendMessageResponseHeader{}
	if response.ExtFields != nil {
		responseHeader.FromMap(response.ExtFields) //change map[string]interface{} into CustomerHeader struct
	}
	sendResult.SetMsgID(message.Properties[constant.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX])
	sendResult.SetOffsetMsgID(responseHeader.MsgId)
	sendResult.SetQueueOffset(responseHeader.QueueOffset)
	sendResult.SetTransactionID(responseHeader.TransactionId)
	messageQueue := model.MessageQueue{Topic: message.Topic, BrokerName: brokerName,
		QueueId: responseHeader.QueueId}
	sendResult.SetMessageQueue(messageQueue)
	var regionId = responseHeader.MsgRegion
	if len(regionId) == 0 {
		regionId = "DefaultRegion"
	}
	sendResult.SetRegionID(regionId)
	return
}

func (self *DefaultProducerService) checkMessage(message *model.Message) (err error) {
	if message == nil {
		err = errors.New("message is nil")
		return
	}
	if len(message.Topic) == 0 {
		err = errors.New("topic is empty")
		return
	}
	if message.Topic == constant.DEFAULT_TOPIC {
		err = errors.New("the topic[" + message.Topic + "] is conflict with default topic.")
		return
	}

	if len(message.Topic) > constant.MAX_MESSAGE_TOPIC_SIZE {
		err = errors.New("the specified topic is longer than topic max length 255.")
		return
	}
	//todo todo     public static final String VALID_PATTERN_STR = "";

	if !util.MatchString(message.Topic, `^[%|a-zA-Z0-9_-]+$`) {
		err = errors.New("the specified topic[" + message.Topic + "] contains illegal characters")
		return
	}
	if len(message.Body) == 0 {
		err = errors.New("messageBody is empty")
		return
	}
	if len(message.Body) > self.producerConfig.MaxMessageSize {
		err = errors.New("messageBody is large than " + util.IntToString(self.producerConfig.MaxMessageSize))
		return
	}
	return
}

func (self *DefaultProducerService) sendMsgUseTopicPublishInfo(message *model.Message, communicationMode string, sendCallback string, topicPublishInfo *model.TopicPublishInfo, timeout int64) (sendResult *model.SendResult, err error) {
	var (
		sendTotalTime int
		messageQueue  model.MessageQueue
	)

	sendTotalTime = 1
	var lastFailedBroker = ""
	//todo transaction
	// todo retry
	for i := 0; i < sendTotalTime; i++ {
		messageQueue, err = selectOneMessageQueue(topicPublishInfo, lastFailedBroker)
		if err != nil {
			return
		}
		sendResult, err = self.doSendMessage(message, messageQueue, communicationMode, sendCallback, topicPublishInfo, timeout)
		if err != nil {
			// todo retry
			return
		}
	}
	return
}

func (self *DefaultProducerService) doSendMessage(message *model.Message, messageQueue model.MessageQueue,
	communicationMode string, sendCallback string,
	topicPublishInfo *model.TopicPublishInfo,
	timeout int64) (sendResult *model.SendResult, err error) {
	var (
		brokerAddr          string
		sysFlag             int
		compressMessageFlag int
	)
	compressMessageFlag, err = self.tryToCompressMessage(message)
	if err != nil {
		return
	}
	sysFlag = sysFlag | compressMessageFlag
	brokerAddr = self.mqClient.FetchMasterBrokerAddress(messageQueue.BrokerName)
	if len(brokerAddr) == 0 {
		err = errors.New("The broker[" + messageQueue.BrokerName + "] not exist")
		return
	}
	message.GeneratorMsgUniqueKey()
	sendMessageHeader := &header.SendMessageRequestHeader{
		ProducerGroup:         self.producerGroup,
		Topic:                 message.Topic,
		DefaultTopic:          constant.DEFAULT_TOPIC,
		DefaultTopicQueueNums: 4,
		QueueId:               messageQueue.QueueId,
		SysFlag:               sysFlag,
		BornTimestamp:         time.Now().UnixNano() / 1000000,
		Flag:                  message.Flag,
		Properties:            util.MessageProperties2String(message.Properties),

		UnitMode:          false,
		ReconsumeTimes:    message.GetReconsumeTimes(),
		MaxReconsumeTimes: message.GetMaxReconsumeTimes(),
	}
	sendResult, err = self.producerSendMessageRequest(brokerAddr, sendMessageHeader, message, timeout)
	return
}

func (self *DefaultProducerService) tryToCompressMessage(message *model.Message) (compressedFlag int, err error) {
	if len(message.Body) < self.producerConfig.CompressMsgBodyOverHowMuch {
		compressedFlag = 0
		return
	}
	compressedFlag = int(constant.CompressedFlag)
	message.Body, err = util.CompressWithLevel(message.Body, self.producerConfig.ZipCompressLevel)
	return
}
