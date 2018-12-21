package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/valyala/fasthttp"
)

var (
	addr     = flag.String("addr", ":80", "TCP address to listen to")
	producer sarama.SyncProducer
)

func main() {
	var err error
	flag.Parse()

	h := requestHandler

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	brokers := []string{"10.133.68.220:9092", "10.133.34.120:9092"}
	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer producer.Close()

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

	msg := &sarama.ProducerMessage{Topic: "loadwo2", Value: sarama.ByteEncoder(body)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Printf("Kafka Error: %s\n", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	fmt.Fprintf(ctx, "{\"status\": \"ok\", \"partition\": %d, \"offset\": %s}", partition, offset)
}
