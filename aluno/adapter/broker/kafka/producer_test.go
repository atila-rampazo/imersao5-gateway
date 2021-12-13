package kafka

import (
	"github.com/atila-rampazo/imersao5-gateway/adapter/presenter/transaction"
	"github.com/atila-rampazo/imersao5-gateway/domain/entity"
	"github.com/atila-rampazo/imersao5-gateway/usecase/process_transaction"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestProducterPublish(t *testing.T) {
	expectOutput := process_transaction.TransactionDtoOutput{
		ID:           "1",
		Status:       entity.REJECTED,
		ErrorMessage: "you dont have limit for this transaction",
	}

	//outputJson, _ := json.Marshal(expectOutput)

	configMap := ckafka.ConfigMap{
		"test.mock.num.brokers": 3,
	}
	producer := NewKafkaProducer(&configMap, transaction.NewTransactionKafkaPresenter())

	err := producer.Publish(expectOutput, []byte("1"), "test")
	assert.Nil(t, err)
}
