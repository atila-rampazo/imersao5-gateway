package main

import (
	"database/sql"
	"encoding/json"
	"github.com/atila-rampazo/imersao5-gateway/adapter/broker/kafka"
	"github.com/atila-rampazo/imersao5-gateway/adapter/factory"
	"github.com/atila-rampazo/imersao5-gateway/adapter/presenter/transaction"
	"github.com/atila-rampazo/imersao5-gateway/usecase/process_transaction"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	_ "github.com/mattn/go-sqlite3"
	"log"
)

func main() {
	db, err := sql.Open("sqlite3", "teste.db")
	if err != nil {
		log.Fatal(err)
	}
	repositoryFactory := factory.NewRepositoryDatabaseFactory(db)
	repository := repositoryFactory.CreateTransactionRepository()

	configMapProducer := &ckafka.ConfigMap{
		"bootstrap.server": "kafka:9092",
	}
	kafkaPresenter := transaction.NewTransactionKafkaPresenter()
	producer := kafka.NewKafkaProducer(configMapProducer, kafkaPresenter)

	var msgChan = make(chan *ckafka.Message)
	confingMapConsumer := &ckafka.ConfigMap{
		"bootstrap.server": "kafka:9092",
		"client.id":        "goapp",
		"group.id":         "goapp",
	}

	topics := []string{"transactions"}
	consumer := kafka.NewConsumer(confingMapConsumer, topics)
	go consumer.Consume(msgChan)

	usecase := process_transaction.NewProcessTransaction(repository, producer, "transaction_result")

	for msg := range msgChan {
		var input process_transaction.TransactionDtoInput
		json.Unmarshal(msg.Value, &input)

		usecase.Execute(input)
	}

}
