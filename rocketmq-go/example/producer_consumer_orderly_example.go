package main

import (
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"strconv"
	"time"

	"github.com/apache/incubator-rocketmq-externals/rocketmq-go"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model"
	"github.com/apache/incubator-rocketmq-externals/rocketmq-go/model/config"
)

var (
	testTopic = "example-topic"
	total     = 256
	n         = 8 // num of queues
)

func main() {
	qs := []int{-8, -7, -6, -5, -4, -3, -2, -1}
	rand.Seed(time.Now().UnixNano())

	go http.ListenAndServe(":6060", nil)

	producer := rocketmq.NewDefaultMQProducer("Test")
	consumer := rocketmq.NewDefaultMQPushConsumer("test-group")
	consumer.ConsumerConfig.PullInterval = 0
	consumer.ConsumerConfig.ConsumeTimeout = 1
	consumer.ConsumerConfig.ConsumeMessageBatchMaxSize = 16
	consumer.ConsumerConfig.ConsumeFromWhere = config.CONSUME_FROM_LAST_OFFSET
	consumer.Subscribe(testTopic, "*")
	consumer.RegisterMessageListenerOrderly(func(msgs []model.MessageExt) model.ConsumeOrderlyResult {
		//simulate randomly error occurs in consuming code
		if (rand.Int())%5 == 0 {
			time.Sleep(time.Second)
			return model.ConsumeOrderlyResult{ConsumeOrderlyStatus: model.SUSPEND_CURRENT_QUEUE_A_MOMENT}
		}
		for _, msg := range msgs {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			i, _ := strconv.Atoi(msg.GetTag())
			j := i % n
			if qs[j]+n != i {
				fmt.Printf("Error: [%d] [%d]\n", qs[j], i)
			}
			fmt.Println(string(msg.Body))
			qs[j] = i
		}
		return model.ConsumeOrderlyResult{ConsumeOrderlyStatus: model.SUCCESS}
	})

	clientConfig := &config.ClientConfig{}
	clientConfig.SetNameServerAddress("193.168.56.4:9876")
	mqManager := rocketmq.MqClientManagerInit(clientConfig)
	mqManager.RegistProducer(producer)
	mqManager.RegistConsumer(consumer)
	mqManager.Start()
	time.Sleep(10 * time.Second)
	for i := 0; i < total; i++ {
		message := &model.Message{}
		message.Topic = testTopic
		message.SetTag(strconv.Itoa(i))
		message.Body = []byte(fmt.Sprintf("{\"name\":\"cc\",\"age\":%d}", i))
		_, err := producer.SendWithSelector(message, select2, i, 3000)
		if err != nil {
			panic(err)
		}
	}
	select {}
}

func select2(queues []model.MessageQueue, message *model.Message, arg interface{}) (queue model.MessageQueue, err error) {
	i := arg.(int)
	queue = queues[i%len(queues)]
	return
}
