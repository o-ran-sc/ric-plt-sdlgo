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
	"github.com/go-redis/redis/v7"
	"io"
	"log"
	"net"
	"os"
	"reflect"
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
	nodeCnt         string
}

type DB struct {
	client       RedisClient
	sentinel     RedisSentinelCreateCb
	subscribe    SubscribeFn
	redisModules bool
	sCbMap       *sharedCbMap
	ch           intChannels
	cfg          Config
	addr         string
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
	Info(section ...string) *redis.StringCmd
}

var dbLogger *log.Logger

func init() {
	dbLogger = log.New(os.Stdout, "database: ", log.LstdFlags|log.Lshortfile)
	redis.SetLogger(dbLogger)
}

func SetDbLogger(out io.Writer) {
	dbLogger.SetOutput(out)
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

func CreateDB(client RedisClient, subscribe SubscribeFn, sentinelCreateCb RedisSentinelCreateCb, cfg Config, sentinelAddr string) *DB {
	db := DB{
		client:       client,
		sentinel:     sentinelCreateCb,
		subscribe:    subscribe,
		redisModules: true,
		sCbMap:       &sharedCbMap{cbMap: make(map[string]ChannelNotificationCb, 0)},
		ch: intChannels{
			addChannel:    make(chan string),
			removeChannel: make(chan string),
			exit:          make(chan bool),
		},
		cfg:  cfg,
		addr: sentinelAddr,
	}

	return &db
}

func Create() []*DB {
	osimpl := osImpl{}
	return ReadConfigAndCreateDbClients(osimpl, newRedisClient, subscribeNotifications, newRedisSentinel)
}

func readConfig(osI OS) Config {
	cfg := Config{
		hostname:        osI.Getenv("DBAAS_SERVICE_HOST", "localhost"),
		port:            osI.Getenv("DBAAS_SERVICE_PORT", "6379"),
		masterName:      osI.Getenv("DBAAS_MASTER_NAME", ""),
		sentinelPort:    osI.Getenv("DBAAS_SERVICE_SENTINEL_PORT", ""),
		clusterAddrList: osI.Getenv("DBAAS_CLUSTER_ADDR_LIST", ""),
		nodeCnt:         osI.Getenv("DBAAS_NODE_COUNT", "1"),
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

func ReadConfigAndCreateDbClients(osI OS, clientCreator RedisClientCreator,
	subscribe SubscribeFn,
	sentinelCreateCb RedisSentinelCreateCb) []*DB {
	cfg := readConfig(osI)
	return createDbClients(cfg, clientCreator, subscribe, sentinelCreateCb)
}

func createDbClients(cfg Config, clientCreator RedisClientCreator,
	subscribe SubscribeFn,
	sentinelCreateCb RedisSentinelCreateCb) []*DB {
	if cfg.clusterAddrList == "" {
		return []*DB{createLegacyDbClient(cfg, clientCreator, subscribe, sentinelCreateCb)}
	}

	dbs := []*DB{}

	addrList := strings.Split(cfg.clusterAddrList, ",")
	for _, addr := range addrList {
		db := createDbClient(cfg, addr, clientCreator, subscribe, sentinelCreateCb)
		dbs = append(dbs, db)
	}
	return dbs
}

func createLegacyDbClient(cfg Config, clientCreator RedisClientCreator,
	subscribe SubscribeFn,
	sentinelCreateCb RedisSentinelCreateCb) *DB {
	return createDbClient(cfg, cfg.hostname, clientCreator, subscribe, sentinelCreateCb)
}

func createDbClient(cfg Config, hostName string, clientCreator RedisClientCreator,
	subscribe SubscribeFn,
	sentinelCreateCb RedisSentinelCreateCb) *DB {
	var client RedisClient
	var db *DB
	if cfg.sentinelPort == "" {
		client = clientCreator(hostName, cfg.port, "", false)
		db = CreateDB(client, subscribe, nil, cfg, hostName)
	} else {
		client = clientCreator(hostName, cfg.sentinelPort, cfg.masterName, true)
		db = CreateDB(client, subscribe, sentinelCreateCb, cfg, hostName)
	}
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
		dbLogger.Printf("SDL DB commands checking failure: %s\n", err)
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
							dbLogger.Printf("SDL DB channel closing failure: %s\n", err)
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

func (db *DB) Info() (*DbInfo, error) {
	var info DbInfo
	resultStr, err := db.client.Info("all").Result()
	if err != nil {
		return &info, err
	}

	result := strings.Split(strings.ReplaceAll(resultStr, "\r\n", "\n"), "\n")
	err = readRedisInfoReplyFields(result, &info)
	return &info, err
}

func lineContains(line, substr string) bool {
	return strings.Contains(line, substr)
}

func getFieldValueStr(line, substr string) string {
	if idx := strings.Index(line, substr); idx != -1 {
		return line[idx+len(substr):]
	}
	return ""
}

func getUint32FromString(s string) uint32 {
	if val, err := strconv.ParseUint(s, 10, 32); err == nil {
		return uint32(val)
	}
	return 0
}

func getUint64FromString(s string) uint64 {
	if val, err := strconv.ParseUint(s, 10, 64); err == nil {
		return uint64(val)
	}
	return 0
}

func getFloatFromString(s string, bitSize int) float64 {
	if val, err := strconv.ParseFloat(s, bitSize); err == nil {
		return val
	}
	return 0
}

func getFloat64FromString(s string) float64 {
	return getFloatFromString(s, 64)
}

func getFloat32FromString(s string) float32 {
	return float32(getFloatFromString(s, 32))
}

func getValueString(values string, key string) string {
	slice := strings.Split(values, ",")
	for _, s := range slice {
		if lineContains(s, key) {
			return getFieldValueStr(s, key)
		}
	}
	return ""
}

func getCommandstatsValues(values string) (string, string, string) {
	calls := getValueString(values, "calls=")
	usec := getValueString(values, "usec=")
	usecPerCall := getValueString(values, "usec_per_call=")
	return calls, usec, usecPerCall
}

func updateCommandstatsValues(i interface{}, line, cmdstat string) {
	stype := reflect.ValueOf(i).Elem()
	values := getFieldValueStr(line, cmdstat)
	callsStr, usecStr, usecPerCallStr := getCommandstatsValues(values)

	callsField := stype.FieldByName("Calls")
	callsField.Set(reflect.ValueOf(getUint32FromString(callsStr)))

	usecField := stype.FieldByName("Usec")
	usecField.Set(reflect.ValueOf(getUint32FromString(usecStr)))

	usecPerCallField := stype.FieldByName("UsecPerCall")
	usecPerCallField.Set(reflect.ValueOf(getFloat32FromString(usecPerCallStr)))
}

func getKeyspaceValues(values string) (string, string, string) {
	keys := getValueString(values, "keys=")
	expires := getValueString(values, "expires=")
	avgttl := getValueString(values, "avg_ttl=")
	return keys, expires, avgttl
}

func updateKeyspaceValues(i interface{}, line, keyspace string) {
	stype := reflect.ValueOf(i).Elem()
	values := getFieldValueStr(line, keyspace)
	keysStr, expiresStr, avgttlStr := getKeyspaceValues(values)

	keysField := stype.FieldByName("Keys")
	keysField.Set(reflect.ValueOf(getUint32FromString(keysStr)))

	expiresField := stype.FieldByName("Expires")
	expiresField.Set(reflect.ValueOf(getUint32FromString(expiresStr)))

	avgttlField := stype.FieldByName("AvgTtl")
	avgttlField.Set(reflect.ValueOf(getUint32FromString(avgttlStr)))
}

func readRedisInfoReplyFields(input []string, info *DbInfo) error {
	for _, line := range input {
		switch {
		case lineContains(line, "role:") && !lineContains(line, "_role:"):
			if "master" == getFieldValueStr(line, "role:") {
				info.Fields.PrimaryRole = true
			}
		case lineContains(line, "connected_slaves:"):
			info.Fields.ConnectedReplicaCnt = getUint32FromString(getFieldValueStr(line, "connected_slaves:"))
		case lineContains(line, "uptime_in_days:"):
			info.Fields.Server.UptimeInDays = getUint32FromString(getFieldValueStr(line, "uptime_in_days:"))
		case lineContains(line, "connected_clients:"):
			info.Fields.Clients.ConnectedClients = getUint32FromString(getFieldValueStr(line, "connected_clients:"))
		case lineContains(line, "client_recent_max_input_buffer:"):
			info.Fields.Clients.ClientRecentMaxInputBuffer = getUint32FromString(getFieldValueStr(line, "client_recent_max_input_buffer:"))
		case lineContains(line, "client_recent_max_output_buffer:"):
			info.Fields.Clients.ClientRecentMaxOutputBuffer = getUint32FromString(getFieldValueStr(line, "client_recent_max_output_buffer:"))
		case lineContains(line, "used_memory:"):
			info.Fields.Memory.UsedMemory = getUint64FromString(getFieldValueStr(line, "used_memory:"))
		case lineContains(line, "used_memory_human:"):
			info.Fields.Memory.UsedMemoryHuman = getFieldValueStr(line, "used_memory_human:")
		case lineContains(line, "used_memory_rss:"):
			info.Fields.Memory.UsedMemoryRss = getUint64FromString(getFieldValueStr(line, "used_memory_rss:"))
		case lineContains(line, "used_memory_rss_human:"):
			info.Fields.Memory.UsedMemoryRssHuman = getFieldValueStr(line, "used_memory_rss_human:")
		case lineContains(line, "used_memory_peak:"):
			info.Fields.Memory.UsedMemoryPeak = getUint64FromString(getFieldValueStr(line, "used_memory_peak:"))
		case lineContains(line, "used_memory_peak_human:"):
			info.Fields.Memory.UsedMemoryPeakHuman = getFieldValueStr(line, "used_memory_peak_human:")
		case lineContains(line, "used_memory_peak_perc:"):
			info.Fields.Memory.UsedMemoryPeakPerc = getFieldValueStr(line, "used_memory_peak_perc:")
		case lineContains(line, "mem_fragmentation_ratio:"):
			info.Fields.Memory.MemFragmentationRatio = getFloat32FromString(getFieldValueStr(line, "mem_fragmentation_ratio:"))
		case lineContains(line, "mem_fragmentation_bytes:"):
			info.Fields.Memory.MemFragmentationBytes = getUint32FromString(getFieldValueStr(line, "mem_fragmentation_bytes:"))
		case lineContains(line, "total_connections_received:"):
			info.Fields.Stats.TotalConnectionsReceived = getUint32FromString(getFieldValueStr(line, "total_connections_received:"))
		case lineContains(line, "total_commands_processed:"):
			info.Fields.Stats.TotalCommandsProcessed = getUint32FromString(getFieldValueStr(line, "total_commands_processed:"))
		case lineContains(line, "sync_full:"):
			info.Fields.Stats.SyncFull = getUint32FromString(getFieldValueStr(line, "sync_full:"))
		case lineContains(line, "sync_partial_ok:"):
			info.Fields.Stats.SyncPartialOk = getUint32FromString(getFieldValueStr(line, "sync_partial_ok:"))
		case lineContains(line, "sync_partial_err:"):
			info.Fields.Stats.SyncPartialErr = getUint32FromString(getFieldValueStr(line, "sync_partial_err:"))
		case lineContains(line, "pubsub_channels:"):
			info.Fields.Stats.PubsubChannels = getUint32FromString(getFieldValueStr(line, "pubsub_channels:"))
		case lineContains(line, "used_cpu_sys:"):
			info.Fields.Cpu.UsedCpuSys = getFloat64FromString(getFieldValueStr(line, "used_cpu_sys:"))
		case lineContains(line, "used_cpu_user:"):
			info.Fields.Cpu.UsedCpuUser = getFloat64FromString(getFieldValueStr(line, "used_cpu_user:"))
		case lineContains(line, "cmdstat_replconf:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatReplconf, line, "cmdstat_replconf:")
		case lineContains(line, "cmdstat_keys:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatKeys, line, "cmdstat_keys:")
		case lineContains(line, "cmdstat_role:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatRole, line, "cmdstat_role:")
		case lineContains(line, "cmdstat_psync:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatPsync, line, "cmdstat_psync:")
		case lineContains(line, "cmdstat_mset:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatMset, line, "cmdstat_mset:")
		case lineContains(line, "cmdstat_publish:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatPublish, line, "cmdstat_publish:")
		case lineContains(line, "cmdstat_info:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatInfo, line, "cmdstat_info:")
		case lineContains(line, "cmdstat_ping:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatPing, line, "cmdstat_ping:")
		case lineContains(line, "cmdstat_client:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatClient, line, "cmdstat_client:")
		case lineContains(line, "cmdstat_command:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatCommand, line, "cmdstat_command:")
		case lineContains(line, "cmdstat_subscribe:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatSubscribe, line, "cmdstat_subscribe:")
		case lineContains(line, "cmdstat_monitor:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatMonitor, line, "cmdstat_monitor:")
		case lineContains(line, "cmdstat_config:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatConfig, line, "cmdstat_config:")
		case lineContains(line, "cmdstat_slaveof:"):
			updateCommandstatsValues(&info.Fields.Commandstats.CmdstatSlaveof, line, "cmdstat_slaveof:")
		case lineContains(line, "db0:"):
			updateKeyspaceValues(&info.Fields.Keyspace.Db, line, "db0:")
		}
	}
	return nil
}

func (db *DB) State() (*DbState, error) {
	dbState := new(DbState)
	if db.cfg.sentinelPort != "" {
		//Establish connection to Redis sentinel. The reason why connection is done
		//here instead of time of the SDL instance creation is that for the time being
		//sentinel connection is needed only here to get state information and
		//state information is needed only by 'sdlcli' hence it is not time critical
		//and also we want to avoid opening unnecessary TCP connections towards Redis
		//sentinel for every SDL instance. Now it is done only when 'sdlcli' is used.
		sentinelClient := db.sentinel(&db.cfg, db.addr)
		return sentinelClient.GetDbState()
	} else {
		info, err := db.Info()
		if err != nil {
			dbState.PrimaryDbState.Err = err
			return dbState, err
		}
		return db.fillDbStateFromDbInfo(info)
	}
}

func (db *DB) fillDbStateFromDbInfo(info *DbInfo) (*DbState, error) {
	var dbState DbState
	if info.Fields.PrimaryRole == true {
		dbState = DbState{
			PrimaryDbState: PrimaryDbState{
				Fields: PrimaryDbStateFields{
					Role:  "master",
					Ip:    db.cfg.hostname,
					Port:  db.cfg.port,
					Flags: "master",
				},
			},
		}
	}

	cnt, err := strconv.Atoi(db.cfg.nodeCnt)
	if err != nil {
		dbState.Err = fmt.Errorf("DBAAS_NODE_COUNT configuration value '%s' conversion to integer failed", db.cfg.nodeCnt)
	} else {
		dbState.ConfigNodeCnt = cnt
	}

	return &dbState, dbState.Err
}

func createReplicaDbClient(host string) *DB {
	cfg := readConfig(osImpl{})
	cfg.sentinelPort = ""
	cfg.clusterAddrList, cfg.port, _ = net.SplitHostPort(host)
	return createDbClient(cfg, cfg.clusterAddrList, newRedisClient, subscribeNotifications, nil)
}

func getStatisticsInfo(db *DB, host string) (*DbStatisticsInfo, error) {
	dbStatisticsInfo := new(DbStatisticsInfo)
	dbStatisticsInfo.IPAddr, dbStatisticsInfo.Port, _ = net.SplitHostPort(host)

	info, err := db.Info()
	if err != nil {
		return nil, err
	}
	dbStatisticsInfo.Info = info

	return dbStatisticsInfo, nil
}

func sentinelStatistics(db *DB) (*DbStatistics, error) {
	dbState := new(DbState)
	dbStatistics := new(DbStatistics)
	dbStatisticsInfo := new(DbStatisticsInfo)
	var err error

	dbState, err = db.State()
	if err != nil {
		return nil, err
	}

	dbStatisticsInfo, err = getStatisticsInfo(db, dbState.PrimaryDbState.GetAddress())
	dbStatistics.Stats = append(dbStatistics.Stats, dbStatisticsInfo)

	if dbState.ReplicasDbState != nil {
		for _, r := range dbState.ReplicasDbState.States {
			replicaDb := createReplicaDbClient(r.GetAddress())
			dbStatisticsInfo, err = getStatisticsInfo(replicaDb, r.GetAddress())
			replicaDb.CloseDB()
			if err != nil {
				return nil, err
			}
			dbStatistics.Stats = append(dbStatistics.Stats, dbStatisticsInfo)
		}
	}

	return dbStatistics, nil
}

func standaloneStatistics(db *DB) (*DbStatistics, error) {
	dbStatistics := new(DbStatistics)

	dbStatisticsInfo, err := getStatisticsInfo(db, net.JoinHostPort(db.cfg.hostname, db.cfg.port))
	dbStatistics.Stats = append(dbStatistics.Stats, dbStatisticsInfo)

	return dbStatistics, err
}

func (db *DB) Statistics() (*DbStatistics, error) {
	if db.cfg.sentinelPort != "" {
		return sentinelStatistics(db)
	}

	return standaloneStatistics(db)
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
