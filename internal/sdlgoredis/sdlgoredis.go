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

/*
 * This source code is part of the near-RT RIC (RAN Intelligent Controller)
 * platform project (RICP).
 */

package sdlgoredis

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ChannelNotificationCb func(channel string, payload ...string)
type RedisClientCreator func(addr, port, clusterName string, isHa bool) RedisClient

type intChannels struct {
	addChannel    chan string
	removeChannel chan string
	exit          chan bool
}

type sharedCbMap struct {
	m     sync.Mutex
	cbMap map[string]ChannelNotificationCb
}

type Config struct {
	hostname        string
	port            string
	masterName      string
	sentinelPort    string
	clusterAddrList string
}

type DB struct {
	client       RedisClient
	subscribe    SubscribeFn
	redisModules bool
	sCbMap       *sharedCbMap
	ch           intChannels
}

type Subscriber interface {
	Channel() <-chan *redis.Message
	Subscribe(channels ...string) error
	Unsubscribe(channels ...string) error
	Close() error
}

type SubscribeFn func(client RedisClient, channels ...string) Subscriber

type RedisClient interface {
	Command() *redis.CommandsInfoCmd
	Close() error
	Subscribe(channels ...string) *redis.PubSub
	MSet(pairs ...interface{}) *redis.StatusCmd
	Do(args ...interface{}) *redis.Cmd
	MGet(keys ...string) *redis.SliceCmd
	Del(keys ...string) *redis.IntCmd
	Keys(pattern string) *redis.StringSliceCmd
	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	SAdd(key string, members ...interface{}) *redis.IntCmd
	SRem(key string, members ...interface{}) *redis.IntCmd
	SMembers(key string) *redis.StringSliceCmd
	SIsMember(key string, member interface{}) *redis.BoolCmd
	SCard(key string) *redis.IntCmd
	PTTL(key string) *redis.DurationCmd
	Eval(script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(script string) *redis.StringCmd
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
	}
	return false, nil
}

func checkIntResultAndError(result interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if n, ok := result.(int64); ok {
		if n == 1 {
			return true, nil
		}
	} else if n, ok := result.(int); ok {
		if n == 1 {
			return true, nil
		}
	}
	return false, nil
}

func subscribeNotifications(client RedisClient, channels ...string) Subscriber {
	return client.Subscribe(channels...)
}

func CreateDB(client RedisClient, subscribe SubscribeFn) *DB {
	db := DB{
		client:       client,
		subscribe:    subscribe,
		redisModules: true,
		sCbMap:       &sharedCbMap{cbMap: make(map[string]ChannelNotificationCb, 0)},
		ch: intChannels{
			addChannel:    make(chan string),
			removeChannel: make(chan string),
			exit:          make(chan bool),
		},
	}

	return &db
}

func Create() []*DB {
	osimpl := osImpl{}
	return ReadConfigAndCreateDbClients(osimpl, newRedisClient)
}

func readConfig(osI OS) Config {
	cfg := Config{
		hostname:        osI.Getenv("DBAAS_SERVICE_HOST", "localhost"),
		port:            osI.Getenv("DBAAS_SERVICE_PORT", "6379"),
		masterName:      osI.Getenv("DBAAS_MASTER_NAME", ""),
		sentinelPort:    osI.Getenv("DBAAS_SERVICE_SENTINEL_PORT", ""),
		clusterAddrList: osI.Getenv("DBAAS_CLUSTER_ADDR_LIST", ""),
	}
	return cfg
}

type OS interface {
	Getenv(key string, defValue string) string
}

type osImpl struct{}

func (osImpl) Getenv(key string, defValue string) string {
	val := os.Getenv(key)
	if val == "" {
		val = defValue
	}
	return val
}

func ReadConfigAndCreateDbClients(osI OS, clientCreator RedisClientCreator) []*DB {
	cfg := readConfig(osI)
	return createDbClients(cfg, clientCreator)
}

func createDbClients(cfg Config, clientCreator RedisClientCreator) []*DB {
	if cfg.clusterAddrList == "" {
		return []*DB{createLegacyDbClient(cfg, clientCreator)}
	}

	dbs := []*DB{}

	addrList := strings.Split(cfg.clusterAddrList, ",")
	for _, addr := range addrList {
		db := createDbClient(cfg, addr, clientCreator)
		dbs = append(dbs, db)
	}
	return dbs
}

func createLegacyDbClient(cfg Config, clientCreator RedisClientCreator) *DB {
	return createDbClient(cfg, cfg.hostname, clientCreator)
}

func createDbClient(cfg Config, hostName string, clientCreator RedisClientCreator) *DB {
	var client RedisClient
	if cfg.sentinelPort == "" {
		client = clientCreator(hostName, cfg.port, "", false)
	} else {
		client = clientCreator(hostName, cfg.sentinelPort, cfg.masterName, true)
	}
	db := CreateDB(client, subscribeNotifications)
	db.CheckCommands()
	return db
}

func newRedisClient(addr, port, clusterName string, isHa bool) RedisClient {
	if isHa == true {
		sentinelAddress := addr + ":" + port
		return redis.NewFailoverClient(
			&redis.FailoverOptions{
				MasterName:    clusterName,
				SentinelAddrs: []string{sentinelAddress},
				PoolSize:      20,
				MaxRetries:    2,
			},
		)
	}
	redisAddress := addr + ":" + port
	return redis.NewClient(&redis.Options{
		Addr:       redisAddress,
		Password:   "", // no password set
		DB:         0,  // use default DB
		PoolSize:   20,
		MaxRetries: 2,
	})
}

func (db *DB) CheckCommands() {
	commands, err := db.client.Command().Result()
	if err == nil {
		redisModuleCommands := []string{"setie", "delie", "setiepub", "setnxpub",
			"msetmpub", "delmpub"}
		for _, v := range redisModuleCommands {
			_, ok := commands[v]
			if !ok {
				db.redisModules = false
			}
		}
	} else {
		fmt.Println(err)
	}
}

func (db *DB) CloseDB() error {
	return db.client.Close()
}

func (db *DB) UnsubscribeChannelDB(channels ...string) {
	for _, v := range channels {
		db.sCbMap.Remove(v)
		db.ch.removeChannel <- v
		if db.sCbMap.Count() == 0 {
			db.ch.exit <- true
		}
	}
}

func (db *DB) SubscribeChannelDB(cb func(string, ...string), channelPrefix, eventSeparator string, channels ...string) {
	if db.sCbMap.Count() == 0 {
		for _, v := range channels {
			db.sCbMap.Add(v, cb)
		}

		go func(sCbMap *sharedCbMap,
			channelPrefix,
			eventSeparator string,
			ch intChannels,
			channels ...string) {
			sub := db.subscribe(db.client, channels...)
			rxChannel := sub.Channel()
			lCbMap := sCbMap.GetMapCopy()
			for {
				select {
				case msg := <-rxChannel:
					cb, ok := lCbMap[msg.Channel]
					if ok {
						cb(strings.TrimPrefix(msg.Channel, channelPrefix), strings.Split(msg.Payload, eventSeparator)...)
					}
				case channel := <-ch.addChannel:
					lCbMap = sCbMap.GetMapCopy()
					sub.Subscribe(channel)
				case channel := <-ch.removeChannel:
					lCbMap = sCbMap.GetMapCopy()
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
		}(db.sCbMap, channelPrefix, eventSeparator, db.ch, channels...)

	} else {
		for _, v := range channels {
			db.sCbMap.Add(v, cb)
			db.ch.addChannel <- v
		}
	}
}

func (db *DB) MSet(pairs ...interface{}) error {
	return db.client.MSet(pairs...).Err()
}

func (db *DB) MSetMPub(channelsAndEvents []string, pairs ...interface{}) error {
	if !db.redisModules {
		return errors.New("Redis deployment doesn't support MSETMPUB command")
	}
	command := make([]interface{}, 0)
	command = append(command, "MSETMPUB")
	command = append(command, len(pairs)/2)
	command = append(command, len(channelsAndEvents)/2)
	for _, d := range pairs {
		command = append(command, d)
	}
	for _, d := range channelsAndEvents {
		command = append(command, d)
	}
	_, err := db.client.Do(command...).Result()
	return err
}

func (db *DB) MGet(keys []string) ([]interface{}, error) {
	return db.client.MGet(keys...).Result()
}

func (db *DB) DelMPub(channelsAndEvents []string, keys []string) error {
	if !db.redisModules {
		return errors.New("Redis deployment not supporting command DELMPUB")
	}
	command := make([]interface{}, 0)
	command = append(command, "DELMPUB")
	command = append(command, len(keys))
	command = append(command, len(channelsAndEvents)/2)
	for _, d := range keys {
		command = append(command, d)
	}
	for _, d := range channelsAndEvents {
		command = append(command, d)
	}
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

func (db *DB) SetIEPub(channelsAndEvents []string, key string, oldData, newData interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command SETIEMPUB")
	}
	capacity := 4 + len(channelsAndEvents)
	command := make([]interface{}, 0, capacity)
	command = append(command, "SETIEMPUB")
	command = append(command, key)
	command = append(command, newData)
	command = append(command, oldData)
	for _, ce := range channelsAndEvents {
		command = append(command, ce)
	}
	return checkResultAndError(db.client.Do(command...).Result())
}

func (db *DB) SetNXPub(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command SETNXMPUB")
	}
	capacity := 3 + len(channelsAndEvents)
	command := make([]interface{}, 0, capacity)
	command = append(command, "SETNXMPUB")
	command = append(command, key)
	command = append(command, data)
	for _, ce := range channelsAndEvents {
		command = append(command, ce)
	}
	return checkResultAndError(db.client.Do(command...).Result())
}
func (db *DB) SetNX(key string, data interface{}, expiration time.Duration) (bool, error) {
	return db.client.SetNX(key, data, expiration).Result()
}

func (db *DB) DelIEPub(channelsAndEvents []string, key string, data interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command DELIEMPUB")
	}
	capacity := 3 + len(channelsAndEvents)
	command := make([]interface{}, 0, capacity)
	command = append(command, "DELIEMPUB")
	command = append(command, key)
	command = append(command, data)
	for _, ce := range channelsAndEvents {
		command = append(command, ce)
	}
	return checkIntResultAndError(db.client.Do(command...).Result())
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

func (db *DB) PTTL(key string) (time.Duration, error) {
	result, err := db.client.PTTL(key).Result()
	return result, err
}

var luaRefresh = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)

func (db *DB) PExpireIE(key string, data interface{}, expiration time.Duration) error {
	expirationStr := strconv.FormatInt(int64(expiration/time.Millisecond), 10)
	result, err := luaRefresh.Run(db.client, []string{key}, data, expirationStr).Result()
	if err != nil {
		return err
	}
	if result == int64(1) {
		return nil
	}
	return errors.New("Lock not held")
}

func (sCbMap *sharedCbMap) Add(channel string, cb ChannelNotificationCb) {
	sCbMap.m.Lock()
	defer sCbMap.m.Unlock()
	sCbMap.cbMap[channel] = cb
}

func (sCbMap *sharedCbMap) Remove(channel string) {
	sCbMap.m.Lock()
	defer sCbMap.m.Unlock()
	delete(sCbMap.cbMap, channel)
}

func (sCbMap *sharedCbMap) Count() int {
	sCbMap.m.Lock()
	defer sCbMap.m.Unlock()
	return len(sCbMap.cbMap)
}

func (sCbMap *sharedCbMap) GetMapCopy() map[string]ChannelNotificationCb {
	sCbMap.m.Lock()
	defer sCbMap.m.Unlock()
	mapCopy := make(map[string]ChannelNotificationCb, 0)
	for i, v := range sCbMap.cbMap {
		mapCopy[i] = v
	}
	return mapCopy
}
