package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

// 连接至kafka后接受消息
func readByConn() {
	// 指定要连接的topic和partition
	topic := "my-topic"
	partition := 0

	// 连接至kafka的leader节点
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	// 设置读取超时时间
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	// 读取一批消息, 得到的batch是一系列消息的迭代器
	batch := conn.ReadBatch(10e3, 1e6)

	// 遍历读取消息
	b := make([]byte, 10e3) // 10kb max per message
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}

	// 关闭batch
	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	// 关闭连接
	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
