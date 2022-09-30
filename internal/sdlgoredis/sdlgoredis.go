/*
   Copyright (c) 2019 AT&T Intellectual Property.
   Copyright (c) 2018-2022 Nokia.

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
	"context"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
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

const EventSeparator = "___"
const NsSeparator = ","

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
	hostname      string
	ports         []string
	masterNames   []string
	sentinelPorts []string
	clusterAddrs  []string
	nodeCnt       string
}

type DB struct {
	ctx          context.Context
	client       RedisClient
	sentinel     RedisSentinelCreateCb
	subscribe    SubscribeFn
	redisModules bool
	sCbMap       *sharedCbMap
	ch           intChannels
	addr         string
	port         string
	sentinelPort string
	masterName   string
	nodeCnt      string
}

type Subscriber interface {
	Channel(opts ...redis.ChannelOption) <-chan *redis.Message
	Subscribe(ctx context.Context, channels ...string) error
	Unsubscribe(ctx context.Context, channels ...string) error
	Close() error
}

type SubscribeFn func(ctx context.Context, client RedisClient, channels ...string) Subscriber

type RedisClient interface {
	Command(ctx context.Context) *redis.CommandsInfoCmd
	Close() error
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
	MSet(ctx context.Context, pairs ...interface{}) *redis.StatusCmd
	Do(ctx context.Context, args ...interface{}) *redis.Cmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Keys(ctx context.Context, pattern string) *redis.StringSliceCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
	SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd
	SCard(ctx context.Context, key string) *redis.IntCmd
	PTTL(ctx context.Context, key string) *redis.DurationCmd
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd
	ScriptExists(ctx context.Context, scripts ...string) *redis.BoolSliceCmd
	ScriptLoad(ctx context.Context, script string) *redis.StringCmd
	Info(ctx context.Context, section ...string) *redis.StringCmd
}

var dbLogger *logger

func init() {
	dbLogger = &logger{
		log: log.New(os.Stdout, "database: ", log.LstdFlags|log.Lshortfile),
	}
	redis.SetLogger(dbLogger)
}

func SetDbLogger(out io.Writer) {
	dbLogger.log.SetOutput(out)
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

func subscribeNotifications(ctx context.Context, client RedisClient, channels ...string) Subscriber {
	return client.Subscribe(ctx, channels...)
}

func CreateDB(client RedisClient, subscribe SubscribeFn, sentinelCreateCb RedisSentinelCreateCb,
	addr, port, sentinelPort, masterName, nodeCnt string) *DB {
	db := DB{
		ctx:          context.Background(),
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
		addr:         addr,
		sentinelPort: sentinelPort,
		port:         port,
		masterName:   masterName,
		nodeCnt:      nodeCnt,
	}

	return &db
}

func Create() []*DB {
	osimpl := osImpl{}
	return ReadConfigAndCreateDbClients(osimpl, newRedisClient, subscribeNotifications, newRedisSentinel)
}

func readConfig(osI OS) Config {
	cfg := Config{
		hostname: osI.Getenv("DBAAS_SERVICE_HOST", "localhost"),
		ports:    strings.Split(osI.Getenv("DBAAS_SERVICE_PORT", "6379"), ","),
		nodeCnt:  osI.Getenv("DBAAS_NODE_COUNT", "1"),
	}

	if addrStr := osI.Getenv("DBAAS_CLUSTER_ADDR_LIST", ""); addrStr != "" {
		cfg.clusterAddrs = strings.Split(addrStr, ",")
	} else if cfg.hostname != "" {
		cfg.clusterAddrs = append(cfg.clusterAddrs, cfg.hostname)
	}
	if sntPortStr := osI.Getenv("DBAAS_SERVICE_SENTINEL_PORT", ""); sntPortStr != "" {
		cfg.sentinelPorts = strings.Split(sntPortStr, ",")
	}
	if nameStr := osI.Getenv("DBAAS_MASTER_NAME", ""); nameStr != "" {
		cfg.masterNames = strings.Split(nameStr, ",")
	}
	completeConfig(&cfg)
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

func completeConfig(cfg *Config) {
	if len(cfg.sentinelPorts) == 0 {
		if len(cfg.clusterAddrs) > len(cfg.ports) && len(cfg.ports) > 0 {
			for i := len(cfg.ports); i < len(cfg.clusterAddrs); i++ {
				cfg.ports = append(cfg.ports, cfg.ports[i-1])
			}
		}
	} else {
		if len(cfg.clusterAddrs) > len(cfg.sentinelPorts) {
			for i := len(cfg.sentinelPorts); i < len(cfg.clusterAddrs); i++ {
				cfg.sentinelPorts = append(cfg.sentinelPorts, cfg.sentinelPorts[i-1])
			}
		}
		if len(cfg.clusterAddrs) > len(cfg.masterNames) && len(cfg.masterNames) > 0 {
			for i := len(cfg.masterNames); i < len(cfg.clusterAddrs); i++ {
				cfg.masterNames = append(cfg.masterNames, cfg.masterNames[i-1])
			}
		}
	}
}

func ReadConfigAndCreateDbClients(osI OS, clientCreator RedisClientCreator,
	subscribe SubscribeFn,
	sentinelCreateCb RedisSentinelCreateCb) []*DB {
	dbs := []*DB{}
	cfg := readConfig(osI)
	for i, addr := range cfg.clusterAddrs {
		port := getListItem(cfg.ports, i)
		sntPort := getListItem(cfg.sentinelPorts, i)
		name := getListItem(cfg.masterNames, i)
		db := createDbClient(addr, port, sntPort, name, cfg.nodeCnt,
			clientCreator, subscribe, sentinelCreateCb)
		dbs = append(dbs, db)
	}
	return dbs
}

func getListItem(list []string, index int) string {
	if index < len(list) {
		return list[index]
	}
	return ""
}

func createDbClient(addr, port, sentinelPort, masterName, nodeCnt string, clientCreator RedisClientCreator,
	subscribe SubscribeFn,
	sentinelCreateCb RedisSentinelCreateCb) *DB {
	var client RedisClient
	var db *DB
	if sentinelPort == "" {
		client = clientCreator(addr, port, "", false)
		db = CreateDB(client, subscribe, nil, addr, port, sentinelPort, masterName, nodeCnt)
	} else {
		client = clientCreator(addr, sentinelPort, masterName, true)
		db = CreateDB(client, subscribe, sentinelCreateCb, addr, port, sentinelPort, masterName, nodeCnt)
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
	commands, err := db.client.Command(db.ctx).Result()
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
		dbLogger.Printf(db.ctx, "SDL DB commands checking failure: %s\n", err)
	}
}

func (db *DB) CloseDB() error {
	return db.client.Close()
}

func (db *DB) UnsubscribeChannelDB(channels ...string) error {
	for _, v := range channels {
		db.sCbMap.Remove(v)
		db.ch.removeChannel <- v
		errStr := <-db.ch.removeChannel
		if errStr != "" {
			return fmt.Errorf("SDL Unsubscribe of channel %s failed: %s", v, errStr)
		}
		if db.sCbMap.Count() == 0 {
			db.ch.exit <- true
		}
	}
	return nil
}

func (db *DB) SubscribeChannelDB(cb func(string, ...string), channels ...string) error {
	if db.sCbMap.Count() == 0 {
		go func(sCbMap *sharedCbMap, ch intChannels) {
			sub := db.subscribe(db.ctx, db.client, "")
			rxChannel := sub.Channel()
			lCbMap := sCbMap.GetMapCopy()
			for {
				select {
				case msg := <-rxChannel:
					cb, ok := lCbMap[msg.Channel]
					if ok {
						nSChNames := strings.SplitAfterN(msg.Channel, NsSeparator, 2)
						cb(nSChNames[1], strings.Split(msg.Payload, EventSeparator)...)
					}
				case channel := <-ch.addChannel:
					lCbMap = sCbMap.GetMapCopy()
					if err := sub.Subscribe(db.ctx, channel); err != nil {
						ch.addChannel <- err.Error()
					} else {
						ch.addChannel <- ""
					}
				case channel := <-ch.removeChannel:
					lCbMap = sCbMap.GetMapCopy()
					if err := sub.Unsubscribe(db.ctx, channel); err != nil {
						ch.removeChannel <- err.Error()
					} else {
						ch.removeChannel <- ""
					}
				case exit := <-ch.exit:
					if exit {
						if err := sub.Close(); err != nil {
							dbLogger.Printf(db.ctx, "SDL DB channel closing failure: %s\n", err)
						}
						return
					}
				}
			}
		}(db.sCbMap, db.ch)
	}
	for _, v := range channels {
		db.sCbMap.Add(v, cb)
		db.ch.addChannel <- v
		errStr := <-db.ch.addChannel
		if errStr != "" {
			return fmt.Errorf("SDL Subscribe of channel %s failed: %s", v, errStr)
		}
	}
	return nil
}

func (db *DB) MSet(pairs ...interface{}) error {
	return db.client.MSet(db.ctx, pairs...).Err()
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
	_, err := db.client.Do(db.ctx, command...).Result()
	return err
}

func (db *DB) MGet(keys []string) ([]interface{}, error) {
	return db.client.MGet(db.ctx, keys...).Result()
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
	_, err := db.client.Do(db.ctx, command...).Result()
	return err

}

func (db *DB) Del(keys []string) error {
	_, err := db.client.Del(db.ctx, keys...).Result()
	return err
}

func (db *DB) Keys(pattern string) ([]string, error) {
	return db.client.Keys(db.ctx, pattern).Result()
}

func (db *DB) SetIE(key string, oldData, newData interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command")
	}

	return checkResultAndError(db.client.Do(db.ctx, "SETIE", key, newData, oldData).Result())
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
	return checkResultAndError(db.client.Do(db.ctx, command...).Result())
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
	return checkResultAndError(db.client.Do(db.ctx, command...).Result())
}
func (db *DB) SetNX(key string, data interface{}, expiration time.Duration) (bool, error) {
	return db.client.SetNX(db.ctx, key, data, expiration).Result()
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
	return checkIntResultAndError(db.client.Do(db.ctx, command...).Result())
}

func (db *DB) DelIE(key string, data interface{}) (bool, error) {
	if !db.redisModules {
		return false, errors.New("Redis deployment not supporting command")
	}
	return checkIntResultAndError(db.client.Do(db.ctx, "DELIE", key, data).Result())
}

func (db *DB) SAdd(key string, data ...interface{}) error {
	_, err := db.client.SAdd(db.ctx, key, data...).Result()
	return err
}

func (db *DB) SRem(key string, data ...interface{}) error {
	_, err := db.client.SRem(db.ctx, key, data...).Result()
	return err
}

func (db *DB) SMembers(key string) ([]string, error) {
	result, err := db.client.SMembers(db.ctx, key).Result()
	return result, err
}

func (db *DB) SIsMember(key string, data interface{}) (bool, error) {
	result, err := db.client.SIsMember(db.ctx, key, data).Result()
	return result, err
}

func (db *DB) SCard(key string) (int64, error) {
	result, err := db.client.SCard(db.ctx, key).Result()
	return result, err
}

func (db *DB) PTTL(key string) (time.Duration, error) {
	result, err := db.client.PTTL(db.ctx, key).Result()
	return result, err
}

func (db *DB) Info() (*DbInfo, error) {
	var info DbInfo
	resultStr, err := db.client.Info(db.ctx, "all").Result()
	if err != nil {
		return &info, err
	}

	result := strings.Split(strings.ReplaceAll(resultStr, "\r\n", "\n"), "\n")
	readRedisInfoReplyFields(result, &info)
	return &info, nil
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

func updateCommandstatsValues(i interface{}, values string) {
	stype := reflect.ValueOf(i).Elem()
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

func updateKeyspaceValues(i interface{}, values string) {
	stype := reflect.ValueOf(i).Elem()
	keysStr, expiresStr, avgttlStr := getKeyspaceValues(values)

	keysField := stype.FieldByName("Keys")
	keysField.Set(reflect.ValueOf(getUint32FromString(keysStr)))

	expiresField := stype.FieldByName("Expires")
	expiresField.Set(reflect.ValueOf(getUint32FromString(expiresStr)))

	avgttlField := stype.FieldByName("AvgTtl")
	avgttlField.Set(reflect.ValueOf(getUint32FromString(avgttlStr)))
}

func updateServerInfoFields(config ConfigInfo, info *DbInfo) {
	if value, ok := config["uptime_in_days"]; ok {
		info.Fields.Server.UptimeInDays = getUint32FromString(value)
	}
}

func updateClientInfoFields(config ConfigInfo, info *DbInfo) {
	if value, ok := config["connected_clients"]; ok {
		info.Fields.Clients.ConnectedClients = getUint32FromString(value)
	}
	if value, ok := config["client_recent_max_input_buffer"]; ok {
		info.Fields.Clients.ClientRecentMaxInputBuffer = getUint32FromString(value)
	}
	if value, ok := config["client_recent_max_output_buffer"]; ok {
		info.Fields.Clients.ClientRecentMaxOutputBuffer = getUint32FromString(value)
	}
}

func updateMemoryInfoFields(config ConfigInfo, info *DbInfo) {
	if value, ok := config["used_memory"]; ok {
		info.Fields.Memory.UsedMemory = getUint64FromString(value)
	}
	if value, ok := config["used_memory_human"]; ok {
		info.Fields.Memory.UsedMemoryHuman = value
	}
	if value, ok := config["used_memory_rss"]; ok {
		info.Fields.Memory.UsedMemoryRss = getUint64FromString(value)
	}
	if value, ok := config["used_memory_rss_human"]; ok {
		info.Fields.Memory.UsedMemoryRssHuman = value
	}
	if value, ok := config["used_memory_peak"]; ok {
		info.Fields.Memory.UsedMemoryPeak = getUint64FromString(value)
	}
	if value, ok := config["used_memory_peak_human"]; ok {
		info.Fields.Memory.UsedMemoryPeakHuman = value
	}
	if value, ok := config["used_memory_peak_perc"]; ok {
		info.Fields.Memory.UsedMemoryPeakPerc = value
	}
	if value, ok := config["mem_fragmentation_ratio"]; ok {
		info.Fields.Memory.MemFragmentationRatio = getFloat32FromString(value)
	}
	if value, ok := config["mem_fragmentation_bytes"]; ok {
		info.Fields.Memory.MemFragmentationBytes = getUint32FromString(value)
	}
}

func updateStatsInfoFields(config ConfigInfo, info *DbInfo) {
	if value, ok := config["total_connections_received"]; ok {
		info.Fields.Stats.TotalConnectionsReceived = getUint32FromString(value)
	}
	if value, ok := config["total_commands_processed"]; ok {
		info.Fields.Stats.TotalCommandsProcessed = getUint32FromString(value)
	}
	if value, ok := config["sync_full"]; ok {
		info.Fields.Stats.SyncFull = getUint32FromString(value)
	}
	if value, ok := config["sync_partial_ok"]; ok {
		info.Fields.Stats.SyncPartialOk = getUint32FromString(value)
	}
	if value, ok := config["sync_partial_err"]; ok {
		info.Fields.Stats.SyncPartialErr = getUint32FromString(value)
	}
	if value, ok := config["pubsub_channels"]; ok {
		info.Fields.Stats.PubsubChannels = getUint32FromString(value)
	}
}

func updateCpuInfoFields(config ConfigInfo, info *DbInfo) {
	if value, ok := config["used_cpu_sys"]; ok {
		info.Fields.Cpu.UsedCpuSys = getFloat64FromString(value)
	}
	if value, ok := config["used_cpu_user"]; ok {
		info.Fields.Cpu.UsedCpuUser = getFloat64FromString(value)
	}
}

func updateCommandstatsInfoFields(config ConfigInfo, info *DbInfo) {
	if values, ok := config["cmdstat_replconf"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatReplconf, values)
	}
	if values, ok := config["cmdstat_keys"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatKeys, values)
	}
	if values, ok := config["cmdstat_role"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatRole, values)
	}
	if values, ok := config["cmdstat_psync"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatPsync, values)
	}
	if values, ok := config["cmdstat_mset"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatMset, values)
	}
	if values, ok := config["cmdstat_publish"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatPublish, values)
	}
	if values, ok := config["cmdstat_info"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatInfo, values)
	}
	if values, ok := config["cmdstat_ping"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatPing, values)
	}
	if values, ok := config["cmdstat_client"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatClient, values)
	}
	if values, ok := config["cmdstat_command"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatCommand, values)
	}
	if values, ok := config["cmdstat_subscribe"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatSubscribe, values)
	}
	if values, ok := config["cmdstat_monitor"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatMonitor, values)
	}
	if values, ok := config["cmdstat_config"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatConfig, values)
	}
	if values, ok := config["cmdstat_slaveof"]; ok {
		updateCommandstatsValues(&info.Fields.Commandstats.CmdstatSlaveof, values)
	}
}

func updateKeyspaceInfoFields(config ConfigInfo, info *DbInfo) {
	if values, ok := config["db0"]; ok {
		updateKeyspaceValues(&info.Fields.Keyspace.Db, values)
	}
}

func getConfigInfo(input []string) ConfigInfo {
	config := ConfigInfo{}
	for _, line := range input {
		if i := strings.Index(line, ":"); i != -1 {
			if key := strings.TrimSpace(line[:i]); len(key) > 0 {
				if len(line) > i {
					config[key] = strings.TrimSpace(line[i+1:])
				}
			}
		}
	}
	return config
}

func readRedisInfoReplyFields(input []string, info *DbInfo) {
	config := getConfigInfo(input)

	if value, ok := config["role"]; ok {
		if "master" == value {
			info.Fields.PrimaryRole = true
		}
	}
	if value, ok := config["connected_slaves"]; ok {
		info.Fields.ConnectedReplicaCnt = getUint32FromString(value)
	}
	updateServerInfoFields(config, info)
	updateClientInfoFields(config, info)
	updateMemoryInfoFields(config, info)
	updateStatsInfoFields(config, info)
	updateCpuInfoFields(config, info)
	updateCommandstatsInfoFields(config, info)
	updateKeyspaceInfoFields(config, info)
}

func (db *DB) State() (*DbState, error) {
	dbState := new(DbState)
	if db.sentinelPort != "" {
		//Establish connection to Redis sentinel. The reason why connection is done
		//here instead of time of the SDL instance creation is that for the time being
		//sentinel connection is needed only here to get state information and
		//state information is needed only by 'sdlcli' hence it is not time critical
		//and also we want to avoid opening unnecessary TCP connections towards Redis
		//sentinel for every SDL instance. Now it is done only when 'sdlcli' is used.
		sentinelClient := db.sentinel(db.addr, db.sentinelPort, db.masterName, db.nodeCnt)
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
					Ip:    db.addr,
					Port:  db.port,
					Flags: "master",
				},
			},
		}
	}

	cnt, err := strconv.Atoi(db.nodeCnt)
	if err != nil {
		dbState.Err = fmt.Errorf("DBAAS_NODE_COUNT configuration value '%s' conversion to integer failed", db.nodeCnt)
	} else {
		dbState.ConfigNodeCnt = cnt
	}

	return &dbState, dbState.Err
}

func createReplicaDbClient(host string) (*DB, error) {
	addr, port, err := net.SplitHostPort(host)
	if err != nil {
		return nil, err
	}
	return createDbClient(addr, port, "", "", "", newRedisClient, subscribeNotifications, nil), err
}

func getStatisticsInfo(db *DB, host string) (*DbStatisticsInfo, error) {
	var err error
	dbStatisticsInfo := new(DbStatisticsInfo)
	dbStatisticsInfo.IPAddr, dbStatisticsInfo.Port, err = net.SplitHostPort(host)
	if err != nil {
		return nil, err
	}

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
			replicaDb, err := createReplicaDbClient(r.GetAddress())
			if err != nil {
				return nil, err
			}
			dbStatisticsInfo, err = getStatisticsInfo(replicaDb, r.GetAddress())
			if closeErr := replicaDb.CloseDB(); closeErr != nil {
				return nil, closeErr
			}
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

	dbStatisticsInfo, err := getStatisticsInfo(db, net.JoinHostPort(db.addr, db.port))
	dbStatistics.Stats = append(dbStatistics.Stats, dbStatisticsInfo)

	return dbStatistics, err
}

func (db *DB) Statistics() (*DbStatistics, error) {
	if db.sentinelPort != "" {
		return sentinelStatistics(db)
	}

	return standaloneStatistics(db)
}

var luaRefresh = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)

func (db *DB) PExpireIE(key string, data interface{}, expiration time.Duration) error {
	expirationStr := strconv.FormatInt(int64(expiration/time.Millisecond), 10)
	result, err := luaRefresh.Run(db.ctx, db.client, []string{key}, data, expirationStr).Result()
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
