package model

type LockBatchRequestBody struct {
	ConsumerGroup string
	ClientId      string
	MqSet         []*MessageQueue
}

type LockBatchResponseBody struct {
	LockOKMQSet []*MessageQueue
}

type UnlockBatchRequestBody struct {
	ConsumerGroup string
	ClientId      string
	MqSet         []*MessageQueue
}
