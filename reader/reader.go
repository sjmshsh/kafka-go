package reader

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

func readerByReader() {
	// 创建Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		Topic:     "topic-A",
		GroupID:   "consumer-group-id", // 指定消费者组id
		Partition: 0,
		MaxBytes:  10e6, // 10MB
	})

	// 接受消息
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
