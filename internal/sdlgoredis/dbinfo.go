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

//DbInfo struct is a holder for DB information, which is received from
//sdlgoredis 'info' call's output.
type DbInfo struct {
	Fields DbInfoFields
}

//DbInfoFields struct is a holder for fields, which are read from sdlgoredis
//'info' call's output.
type DbInfoFields struct {
	PrimaryRole         bool
	ConnectedReplicaCnt uint32
	Server              ServerInfoFields
	Clients             ClientsInfoFields
	Memory              MeroryInfoFields
	Stats               StatsInfoFields
	Cpu                 CpuInfoFields
	Commandstats        CommandstatsInfoFields
	Keyspace            KeyspaceInfoFields
}

type ServerInfoFields struct {
	UptimeInDays uint32
}

type ClientsInfoFields struct {
	ConnectedClients            uint32
	ClientRecentMaxInputBuffer  uint32
	ClientRecentMaxOutputBuffer uint32
}

type MeroryInfoFields struct {
	UsedMemory            uint64
	UsedMemoryHuman       string
	UsedMemoryRss         uint64
	UsedMemoryRssHuman    string
	UsedMemoryPeak        uint64
	UsedMemoryPeakHuman   string
	UsedMemoryPeakPerc    string
	MemFragmentationRatio float32
	MemFragmentationBytes uint32
}

type StatsInfoFields struct {
	TotalConnectionsReceived uint32
	TotalCommandsProcessed   uint32
	SyncFull                 uint32
	SyncPartialOk            uint32
	SyncPartialErr           uint32
	PubsubChannels           uint32
}

type CpuInfoFields struct {
	UsedCpuSys  float64
	UsedCpuUser float64
}

type CommandstatsValues struct {
	Calls       uint32
	Usec        uint32
	UsecPerCall float32
}

type CommandstatsInfoFields struct {
	CmdstatReplconf  CommandstatsValues
	CmdstatKeys      CommandstatsValues
	CmdstatRole      CommandstatsValues
	CmdstatConfig    CommandstatsValues
	CmdstatPsync     CommandstatsValues
	CmdstatMset      CommandstatsValues
	CmdstatPublish   CommandstatsValues
	CmdstatInfo      CommandstatsValues
	CmdstatPing      CommandstatsValues
	CmdstatClient    CommandstatsValues
	CmdstatCommand   CommandstatsValues
	CmdstatSubscribe CommandstatsValues
	CmdstatMonitor   CommandstatsValues
	CmdstatSlaveof   CommandstatsValues
}

type KeyspaceValues struct {
	Keys    uint32
	Expires uint32
	AvgTtl  uint32
}

type KeyspaceInfoFields struct {
	Db KeyspaceValues
}

type ConfigInfo map[string]string
