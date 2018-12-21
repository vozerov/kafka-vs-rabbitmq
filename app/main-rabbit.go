package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/streadway/amqp"
	"github.com/valyala/fasthttp"
)

var (
	addr  = flag.String("addr", ":80", "TCP address to listen to")
	queue amqp.Queue
	ch    *amqp.Channel
)

func main() {
	var err error
	flag.Parse()

	h := requestHandler

	conn, err := amqp.Dial("amqp://load:load@10.133.68.220:5672/")
	if err != nil {
		log.Fatalf("Can't connect to rabbitmq")
	}
	defer conn.Close()

	ch, err = conn.Channel()
	if err != nil {
		log.Fatalf("Can't open channel")
	}
	defer ch.Close()

	queue, err = ch.QueueDeclare(
		"load", // name
		true,   // durable - flush to disk
		false,  // delete when unused
		false,  // exclusive - only accessible by the connection that declares
		false,  // no-wait - the queue will assume to be declared on the server
		nil,    // arguments -
	)
	if err != nil {
		log.Fatalf("Can't create queue")
	}

	if err := fasthttp.ListenAndServe(*addr, h); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}

func isJSON(s []byte) bool {
	var js map[string]interface{}
	return json.Unmarshal(s, &js) == nil
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	if string(ctx.Path()) == "/loaderio-1ed31b14bcaaff1bd3e7eb556028c54c.txt" {
		fmt.Fprintf(ctx, "loaderio-1ed31b14bcaaff1bd3e7eb556028c54c")
		return
	}

	body := ctx.PostBody()
	// Checking if json is correct
	if !isJSON(body) {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		return
	}

	err := ch.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory - could return an error if there are no consumers or queue
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         body,
		})

	if err != nil {
		fmt.Printf("RabbitMQ Error: %s\n", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	fmt.Fprintf(ctx, "{\"status\": \"ok\"}")
}
