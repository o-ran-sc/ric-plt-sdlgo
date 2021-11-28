/*
   Copyright (c) 2021 AT&T Intellectual Property.
   Copyright (c) 2018-2021 Nokia.

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
	"github.com/go-redis/redis/v7"
	"strconv"
)

type Sentinel struct {
	IredisSentinelClient
	Cfg *Config
}

type IredisSentinelClient interface {
	Master(name string) *redis.StringStringMapCmd
	Slaves(name string) *redis.SliceCmd
	Sentinels(name string) *redis.SliceCmd
}

type RedisSentinelCreateCb func(cfg *Config, addr string) *Sentinel

func newRedisSentinel(cfg *Config, addr string) *Sentinel {
	redisAddress := addr + ":" + cfg.sentinelPort
	return &Sentinel{
		IredisSentinelClient: redis.NewSentinelClient(&redis.Options{
			Addr:       redisAddress,
			Password:   "", // no password set
			DB:         0,  // use default DB
			PoolSize:   20,
			MaxRetries: 2,
		}),
		Cfg: cfg,
	}
}

func (s *Sentinel) GetDbState() (*DbState, error) {
	state := new(DbState)
	state.ConfigNodeCnt, state.Err = strconv.Atoi(s.Cfg.nodeCnt)
	pState, pErr := s.getPrimaryDbState()
	rState, rErr := s.getReplicasState()
	sState, sErr := s.getSentinelsState()
	state.PrimaryDbState = *pState
	state.ReplicasDbState = rState
	state.SentinelsDbState = sState

	if state.Err != nil {
		return state, state.Err
	}

	if pErr != nil {
		return state, pErr
	}
	if rErr != nil {
		return state, rErr
	}
	return state, sErr
}

func (s *Sentinel) getPrimaryDbState() (*PrimaryDbState, error) {
	state := new(PrimaryDbState)
	redisVal, redisErr := s.Master(s.Cfg.masterName).Result()
	if redisErr == nil {
		state.Fields.Ip = redisVal["ip"]
		state.Fields.Port = redisVal["port"]
		state.Fields.Flags = redisVal["flags"]
		state.Fields.Role = redisVal["role-reported"]
	}
	state.Err = redisErr
	return state, redisErr
}

func (s *Sentinel) getReplicasState() (*ReplicasDbState, error) {
	states := new(ReplicasDbState)
	states.States = make([]*ReplicaDbState, 0)

	redisVal, redisErr := s.Slaves(s.Cfg.masterName).Result()
	if redisErr == nil {
		for _, redisReplica := range redisVal {
			replicaState := readReplicaState(redisReplica.([]interface{}))
			states.States = append(states.States, replicaState)
		}
	}
	states.Err = redisErr
	return states, redisErr
}

func readReplicaState(redisReplicas []interface{}) *ReplicaDbState {
	state := new(ReplicaDbState)
	for i := 0; i < len(redisReplicas); i += 2 {
		if redisReplicas[i].(string) == "ip" {
			state.Fields.Ip = redisReplicas[i+1].(string)
		} else if redisReplicas[i].(string) == "port" {
			state.Fields.Port = redisReplicas[i+1].(string)
		} else if redisReplicas[i].(string) == "flags" {
			state.Fields.Flags = redisReplicas[i+1].(string)
		} else if redisReplicas[i].(string) == "role-reported" {
			state.Fields.Role = redisReplicas[i+1].(string)
		} else if redisReplicas[i].(string) == "master-link-status" {
			state.Fields.PrimaryLinkStatus = redisReplicas[i+1].(string)
		}
	}
	return state
}

func (s *Sentinel) getSentinelsState() (*SentinelsDbState, error) {
	states := new(SentinelsDbState)
	states.States = make([]*SentinelDbState, 0)

	redisVal, redisErr := s.Sentinels(s.Cfg.masterName).Result()
	if redisErr == nil {
		for _, redisSentinel := range redisVal {
			sentinelState := readSentinelState(redisSentinel.([]interface{}))
			states.States = append(states.States, sentinelState)
		}
	}
	states.Err = redisErr
	return states, redisErr
}

func readSentinelState(redisSentinels []interface{}) *SentinelDbState {
	state := new(SentinelDbState)
	for i := 0; i < len(redisSentinels); i += 2 {
		if redisSentinels[i].(string) == "ip" {
			state.Fields.Ip = redisSentinels[i+1].(string)
		} else if redisSentinels[i].(string) == "port" {
			state.Fields.Port = redisSentinels[i+1].(string)
		} else if redisSentinels[i].(string) == "flags" {
			state.Fields.Flags = redisSentinels[i+1].(string)
		}
	}
	return state
}
