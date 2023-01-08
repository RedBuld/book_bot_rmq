# book_bot_rmq
RabbitMQ for Book Bot

пример использования

	func (DC *DownloadCenter) initRMQ() {
		rmq_params := &bbr.RMQ_Params{
			Server: "amqp://guest:guest@localhost:5672/",
			Queue: &bbr.RMQ_Params_Queue{
				Name:    "elib_fb2_downloads",
				Durable: true,
				// AutoAck: true,
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
			Consumer: DC.onMessage,
		}
		rmq := bbr.NewRMQ(rmq_params)
		defer rmq.Close()

		DC.rmq = rmq
	}
	func (DC *DownloadCenter) SendStatus(RoutingKey string) {
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
