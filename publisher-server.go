package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"os"
	"strconv"
	"sync"
	"github.com/ably/ably-go/ably"
	"time"
)

var ctx   =  context.Background()

var wg 	  =  &sync.WaitGroup{}

func main() {
	client   := getRedis()
	channel  := getAblyChannel()

	go func() {
		for {
			transactioWithRedis(client,channel)
		}
	}()

	wg.Add(1)
	wg.Wait()
}

func getRedis() *redis.Client {
	var (
		host 	 	=   getEnv("REDIS_HOST","localhost")
		port 	 	= 	string(getEnv("REDIS_PORT","6379"))
		password 	= 	getEnv("REDIS_PASSWORD","")
	)
	client  := 	 redis.NewClient(&redis.Options{
		Addr: 	  host+":"+port,
		Password: password,
		DB: 	  0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}
	return client
}

func getAblyChannel() *ably.RealtimeChannel {
	ablyClient, err := ably.NewRealtime(ably.WithKey(getEnv("ABLY_KEY","cUPF8Q.FIUUsA:iBdHSjSNBULn56mU")))

	if err != nil {
		panic(err)
	}

	return ablyClient.Channels.Get(getEnv("CHANNEL_NAME","trades"))
}

func transactioWithRedis(client *redis.Client, channel *ably.RealtimeChannel) error {
	redisQueueName := getEnv("QUEUE_KEY","myJobQueue")

	redisLogName   := redisQueueName + ":log"
	now  	 	   := time.Now().UnixNano()
	windowsSize    := int64(time.Second)
	clearBefore    := now - windowsSize
	rateLimit, _   := strconv.ParseInt(getEnv("RATE_LIMIT","50"),10,64)

	err := client.Watch(func(tx *redis.Tx) error {
		tx.ZRemRangeByScore(redisLogName, "0", strconv.FormatInt(clearBefore, 10))

		// Get the number of messages sent this second
		messagesThisSecond, err := tx.ZCard(redisLogName).Result()
		if err != nil && err != redis.Nil {
			return err
		}

		// If under rate limit, indicate that we'll be publishing another message
		// And publish it to Ably
		if messagesThisSecond < rateLimit {
			err = tx.ZAdd(redisLogName, redis.Z{
				Score:  float64(now),
				Member: now,
			}).Err()
			if err != nil && err != redis.Nil {
				return err
			}

			messageToPublish, err := tx.BLPop(0*time.Second, redisQueueName).Result()
			if err != nil && err != redis.Nil {
				return err
			}

			err = channel.Publish(ctx, "trade", messageToPublish[1])
			if err != nil {
				fmt.Println(err)
			}
		}

		return err
	}, redisLogName)

	return  err
}

func getEnv(envName, valueDefault string) string {
	value := os.Getenv(envName)
	if value == "" {
		return valueDefault
	}
	return value
}