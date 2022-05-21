package msghelper

import (
	"github.com/Golang-Tools/optparams"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//ExtractValue 从消息中获取value
//@params msg *kafka.Message 消息指针
func ExtractValue(msg *kafka.Message) ([]byte, error) {
	if msg.TopicPartition.Error != nil {
		return nil, msg.TopicPartition.Error
	}
	return msg.Value, nil
}

//ExtractValue 从消息中获取key
//@params msg *kafka.Message 消息指针
func ExtractKey(msg *kafka.Message) ([]byte, error) {
	if msg.TopicPartition.Error != nil {
		return nil, msg.TopicPartition.Error
	}
	return msg.Key, nil
}

//ExtractValue 从消息中获取headers字典
//@params msg *kafka.Message 消息指针
func ExtractHeaders(msg *kafka.Message) (map[string][]byte, error) {
	if msg.TopicPartition.Error != nil {
		return nil, msg.TopicPartition.Error
	}
	result := map[string][]byte{}
	for _, header := range msg.Headers {
		result[header.Key] = header.Value
	}
	return result, nil
}

//ExtractTopic 从消息中获取来源的topic
//@params msg *kafka.Message 消息指针
func ExtractTopic(msg *kafka.Message) string {
	return *msg.TopicPartition.Topic
}

//ConciseMsg 简化版本的消息对象,*kafka.Message信息过全,结构略复杂并不太利于利用
type ConciseMsg struct {
	Topic   string
	Value   []byte
	Key     []byte
	Headers map[string][]byte
}

//AsMessage 将精简消息转化为kafka消息
//@params opts ...optparams.Option[kafka.Message] 只可以使用参数WithPartition用于指定希望发送去的分区
func (c *ConciseMsg) AsMessage(opts ...optparams.Option[kafka.Message]) *kafka.Message {
	result := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &c.Topic, Partition: kafka.PartitionAny},
		Value:          c.Value,
	}
	if c.Key != nil {
		result.Key = c.Key
	}
	if len(c.Headers) > 0 {
		hs := []kafka.Header{}
		for k, v := range c.Headers {
			hs = append(hs, kafka.Header{Key: k, Value: v})
		}
		result.Headers = hs
	}
	optparams.GetOption(&result, opts...)
	return &result
}

//Extract 从消息中提取精简信息
func Extract(msg *kafka.Message) (*ConciseMsg, error) {
	if msg.TopicPartition.Error != nil {
		return nil, msg.TopicPartition.Error
	}
	hs := map[string][]byte{}
	for _, header := range msg.Headers {
		hs[header.Key] = header.Value
	}

	result := ConciseMsg{
		Topic:   *msg.TopicPartition.Topic,
		Value:   msg.Value,
		Key:     msg.Key,
		Headers: hs,
	}
	return &result, nil
}

//NewMsg 构造消息用于发送
//@params topic string 消息要发送去的topic
//@params value []byte 消息的值
//@params opts ...optparams.Option[kafka.Message] 消息的其他设置
func NewMsg(topic string, value []byte, opts ...optparams.Option[kafka.Message]) *kafka.Message {
	msg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}
	optparams.GetOption(&msg, opts...)
	return &msg
}
