/*
   Copyright (c) 2019 AT&T Intellectual Property.
   Copyright (c) 2018-2019 Nokia.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package sdlgoredis

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-redis/redis"
)

type ChannelNotificationCb func(channel string, payload ...string)

type intChannels struct {
	addChannel    chan string
	removeChannel chan string
	exit          chan bool
}

type DB struct {
	client       *redis.Client
	redisModules bool
	cbMap        map[string]ChannelNotificationCb
	ch           intChannels
}

func checkResultAndError(result interface{}, err error) (bool, error) {
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	if result == "OK" {
		return true, nil
	} else {
		return false, nil
	}
}

func checkIntResultAndError(result interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if result.(int64) == 1 {
		return true, nil
	} else {
		return false, nil
	}

}

func Create() *DB {
	hostname := os.Getenv("DBAAS_SERVICE_HOST")
	if hostname == "" {
		hostname = "localhost"
	}
	port := os.Getenv("DBAAS_SERVICE_PORT")
	if port == "" {
		port = "6379"
	}
	redisAddress := hostname + ":" + port
	client := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
		PoolSize: 20,
	})

	db := DB{
		client:       client,
		redisModules: true,
		cbMap:        make(map[string]ChannelNotificationCb, 0),
		ch: intChannels{
			addChannel:    make(chan string),
			removeChannel: make(chan string),
			exit:          make(chan bool),
		},
	}

	commands, err := db.client.Command().Result()
	if err == nil {
		redisModuleCommands := []string{"setie", "delie", "msetpub", "setiepub", "setnxpub", "delpub"}
		for _, v := range redisModuleCommands {
			_, ok := commands[v]
			if !ok {
				db.redisModules = false
			}
		}
	} else {
		fmt.Println(err)
	}
	return &db
}

func (db *DB) CloseDB() error {
	return db.client.Close()
}

func (db *DB) UnsubscribeChannelDB(channels ...string) {
	for _, v := range channels {
		db.ch.removeChannel <- v
		delete(db.cbMap, v)
		if len(db.cbMap) == 0 {
			db.ch.exit <- true
		}
	}
}

func (db *DB) SubscribeChannelDB(cb ChannelNotificationCb, channelPrefix, eventSeparator string, channels ...string) {
	if len(db.cbMap) == 0 {
		for _, v := range channels {
			db.cbMap[v] = cb
		}

		go func(cbMap *map[string]ChannelNotificationCb,
			channelPrefix,
			eventSeparator string,
			ch intChannels,
			channels ...string) {
			sub := db.client.Subscribe(channels...)
			rxChannel := sub.Channel()
			for {
				select {
				case msg := <-rxChannel:
					cb, ok := (*cbMap)[msg.Channel]
					if ok {
						cb(strings.TrimPrefix(msg.Channel, channelPrefix), strings.Split(msg.Payload, eventSeparator)...)
					}
				case channel := <-ch.addChannel:
					sub.Subscribe(channel)
				case channel := <-ch.removeChannel:
					sub.Unsubscribe(channel)
				case exit := <-ch.exit:
					if exit {
						if err := sub.Close(); err != nil {
							fmt.Println(err)
						}
						return
					}
				}
			}
		}(&db.cbMap, channelPrefix, eventSeparator, db.ch, channels...)

	} else {
		for _, v := range channels {
			db.cbMap[v] = cb
			db.ch.addChannel <- v
		}
	}
}

func (db *DB) MSet(pairs ...interface{}) error {
	return db.client.MSet(pairs...).Err()
}

func (db *DB) MSetPub(channel, message string, pairs ...interface{}) error {
	if !db.redisModules {
		return errors.New("Redis deployment doesn't support MSETPUB command")
	}
	command := make([]interface{}, 0)
	command = append(command, "MSETPUB")
	for _, d := range pairs {
		command = append(command, d)
	}
	command = append(command, channel, message)
	_, err := db.client.Do(command...).Result()
	return err
}

func (db *DB) MGet(keys []string) ([]interface{}, error) {
	return db.client.MGet(keys...).Result()
}

func (db *DB) DelPub(channel, message string, keys []string) error {
	if !db.redisModules {
		return errors.New("Redis deployment not supporting command DELPUB")
	}
	command := make([]interface{}, 0)
	command = append(command, "DELPUB")
	for _, d := range keys {
		command = append(command, d)
	}
	command = append(command, channel, message)
	_, err := db.client.Do(command...).Result()
	return err
}

func (db *DB) Del(keys []string) error {
	_, err := db.client.Del(keys...).Result()
	return err
}

func (db *DB) Keys(pattern string) ([]string, error) {
	return db.client.Keys(pattern).Result()
}

func (db *DB) SetIE(key string, oldData, newData interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command")
	}

	return checkResultAndError(db.client.Do("SETIE", key, newData, oldData).Result())
}

func (db *DB) SetIEPub(channel, message, key string, oldData, newData interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command SETIEPUB")
	}
	return checkResultAndError(db.client.Do("SETIEPUB", key, newData, oldData, channel, message).Result())
}

func (db *DB) SetNXPub(channel, message, key string, data interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command SETNXPUB")
	}
	return checkResultAndError(db.client.Do("SETNXPUB", key, data, channel, message).Result())
}
func (db *DB) SetNX(key string, data interface{}) (bool, error) {
	return db.client.SetNX(key, data, 0).Result()
}

func (db *DB) DelIEPub(channel, message, key string, data interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command")
	}
	return checkIntResultAndError(db.client.Do("DELIEPUB", key, data, channel, message).Result())
}

func (db *DB) DelIE(key string, data interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command")
	}
	return checkIntResultAndError(db.client.Do("DELIE", key, data).Result())
}

func (db *DB) SAdd(key string, data ...interface{}) error {
	_, err := db.client.SAdd(key, data...).Result()
	return err
}

func (db *DB) SRem(key string, data ...interface{}) error {
	_, err := db.client.SRem(key, data...).Result()
	return err
}

func (db *DB) SMembers(key string) ([]string, error) {
	result, err := db.client.SMembers(key).Result()
	return result, err
}

func (db *DB) SIsMember(key string, data interface{}) (bool, error) {
	result, err := db.client.SIsMember(key, data).Result()
	return result, err
}

func (db *DB) SCard(key string) (int64, error) {
	result, err := db.client.SCard(key).Result()
	return result, err
}
