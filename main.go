package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Azure/azure-service-bus-go"
	"github.com/Azure/go-amqp"
	"github.com/joho/godotenv"
)

type MessageHandler struct{}

func (mh *MessageHandler) Handle(ctx context.Context, msg *servicebus.Message) error {
	fmt.Printf("-> Received message: %s\n", string(msg.Data))

	// Processing of message simulated through delay
	time.Sleep(5 * time.Second)

	fmt.Printf("✔ Finished processing the message: %s\n", string(msg.Data))
	return msg.Complete(ctx)
}

func main() {
	// Read env variables from .env file if it exists
	loadEnvFromFileIfExists()

	handler := &MessageHandler{}

	// Set background context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	connStr := os.Getenv("SERVICEBUS_CONNECTION_STRING")
	qName := os.Getenv("QUEUE_NAME")
	if connStr == "" || qName == "" {
		fmt.Println("FATAL: expected environment variables SERVICEBUS_CONNECTION_STRING or QUEUE_NAME not set")
		return
	}

	// Create a client to communicate with a Service Bus Namespace.
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create queue receiver
	q, err := ns.NewQueue(qName)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		if err = q.ReceiveOne(ctx, handler); err != nil {
			if innerErr, ok := err.(*amqp.Error); ok && innerErr.Condition == "com.microsoft:timeout" {
				fmt.Println("➰ Timeout waiting for messages. Entering next loop.")
				continue
			}

			fmt.Println(err)
			return
		}
	}
}

func loadEnvFromFileIfExists() {
	envFile := ".env"
	if _, err := os.Stat(envFile); err == nil {
		if err = godotenv.Load(envFile); err != nil {
			log.Fatalf("Error loading .env file")
		}
	}
}
