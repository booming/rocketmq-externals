package rocketmq

import (
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/config"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/constant"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/service"
)

type RocketMQProducer interface {
	Send(message *model.Message) (sendResult *model.SendResult, err error)
	SendWithTimeout(message *model.Message, timeout int64) (sendResult *model.SendResult, err error)
	SendToQueue(message *model.Message, queue model.MessageQueue, timeout int64) (sendResult *model.SendResult, err error)
	SendWithSelector(message *model.Message, selector service.MessageQueueSelector, arg interface{}, timeout int64) (sendResult *model.SendResult, err error)
	//SendAsync(message *model.Message) (sendResult *model.SendResult,err error)
	//SendAsyncWithTimeout(message *model.Message) (sendResult *model.SendResult,err error)
	//SendOneWay(message *model.Message) (sendResult *model.SendResult,err error)
}
type DefaultMQProducer struct {
	producerGroup  string
	ProducerConfig *config.RocketMqProducerConfig

	producerService service.ProducerService
}

func NewDefaultMQProducer(producerGroup string) (rocketMQProducer *DefaultMQProducer) {
	rocketMQProducer = &DefaultMQProducer{
		producerGroup:  producerGroup,
		ProducerConfig: config.NewProducerConfig(),
	}
	return
}

func (self *DefaultMQProducer) Send(message *model.Message) (sendResult *model.SendResult, err error) {
	sendResult, err = self.producerService.SendDefaultImpl(message, constant.COMMUNICATIONMODE_SYNC, "", self.ProducerConfig.SendMsgTimeout)
	return
}
func (self *DefaultMQProducer) SendWithTimeout(message *model.Message, timeout int64) (sendResult *model.SendResult, err error) {
	sendResult, err = self.producerService.SendDefaultImpl(message, constant.COMMUNICATIONMODE_SYNC, "", timeout)
	return
}

func (self *DefaultMQProducer) SendToQueue(message *model.Message, queue model.MessageQueue, timeout int64) (sendResult *model.SendResult, err error) {
	sendResult, err = self.producerService.SendQueueImpl(message, queue, constant.COMMUNICATIONMODE_SYNC, "", timeout)
	return
}

func (self *DefaultMQProducer) SendWithSelector(message *model.Message, selector service.MessageQueueSelector, arg interface{}, timeout int64) (sendResult *model.SendResult, err error) {
	sendResult, err = self.producerService.SendSelectImpl(message, selector, arg, constant.COMMUNICATIONMODE_SYNC, "", timeout)
	return
}
