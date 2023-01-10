# book_bot_rmq
RabbitMQ for Book Bot

пример использования
```
package main

import (
    bbr "github.com/RedBuld/book_bot_rmq"
)

func main() {
	rmq_params := &bbr.RMQ_Params{
		Server: "amqp://guest:guest@localhost:5672/",
		Queue: &bbr.RMQ_Params_Queue{
			Name:    "elib_fb2_downloads",
			Durable: true,
		},
		Exchange: &bbr.RMQ_Params_Exchange{
			Name:       "download_requests",
			Mode:       "topic",
			RoutingKey: "*",
			Durable:    true,
		},
		Prefetch: &bbr.RMQ_Params_Prefetch{
			Count:  1,
			Size:   0,
			Global: false,
		},
		Consumer: onMessage,
	}
	rmq := bbr.NewRMQ(rmq_params)
	defer rmq.Close()

	DC.rmq = rmq
}

func onMessage(message amqp.Delivery) {
	fmt.Printf("[%s] Message [%s]: %s\n", time.Now(), message.RoutingKey, message.Body)
	message.Ack(false)
	SendStatus(message.RoutingKey)
}

func SendStatus(RoutingKey string) {
	fmt.Println("Sending download status")

	message := &bbr.RMQ_Message{
		Exchange:   "download_statuses",
		RoutingKey: RoutingKey,
		Mandatory:  false,
		Immediate:  false,
		Params: amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte("Download accepted"),
		},
	}
	err := DC.rmq.Push(message)
	if err != nil {
		panic(err)
	}

	fmt.Println("Sended download status")
}
```
