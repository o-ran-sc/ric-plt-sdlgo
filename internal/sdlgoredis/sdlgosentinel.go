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
	mState, mErr := s.getMasterDbState()
	rState, rErr := s.getReplicasState()
	state.MasterDbState = *mState
	state.ReplicasDbState = rState
	if mErr == nil {
		return state, rErr
	}
	return state, mErr
}

func (s *Sentinel) getMasterDbState() (*MasterDbState, error) {
	var cnt int
	state := new(MasterDbState)
	redisVal, redisErr := s.Master(s.Cfg.masterName).Result()
	if redisErr == nil {
		state.Fields.Ip = redisVal["ip"]
		state.Fields.Port = redisVal["port"]
		state.Fields.Flags = redisVal["flags"]
		state.Fields.Role = redisVal["role-reported"]
		state.Fields.ReplicasCnt = redisVal["num-slaves"]

		if cnt, redisErr = strconv.Atoi(s.Cfg.nodeCount); redisErr == nil && cnt > 0 {
			//Configuration nodeCount contains master and slave nodes count, that's
			//why decrement by one below
			state.Fields.ReplicasCfgCnt = strconv.Itoa(cnt - 1)
		}
	}
	state.Err = redisErr
	return state, redisErr
}

func (s *Sentinel) getReplicasState() (*ReplicasDbState, error) {
	states := new(ReplicasDbState)
	states.States = make([]*ReplicaDbState, 0)

	redisVal, redisErr := s.Slaves(s.Cfg.masterName).Result()
	if redisErr == nil {
		for _, redisSlave := range redisVal {
			replicaState := readReplicaState(redisSlave.([]interface{}))
			states.States = append(states.States, replicaState)
		}
	}
	states.Err = redisErr
	return states, redisErr
}

func readReplicaState(redisSlaves []interface{}) *ReplicaDbState {
	state := new(ReplicaDbState)
	for i := 0; i < len(redisSlaves); i += 2 {
		if redisSlaves[i].(string) == "ip" {
			state.Fields.Ip = redisSlaves[i+1].(string)
		} else if redisSlaves[i].(string) == "port" {
			state.Fields.Port = redisSlaves[i+1].(string)
		} else if redisSlaves[i].(string) == "flags" {
			state.Fields.Flags = redisSlaves[i+1].(string)
		} else if redisSlaves[i].(string) == "role-reported" {
			state.Fields.Role = redisSlaves[i+1].(string)
		} else if redisSlaves[i].(string) == "master-link-status" {
			state.Fields.MasterLinkStatus = redisSlaves[i+1].(string)
		}
	}
	return state
}
