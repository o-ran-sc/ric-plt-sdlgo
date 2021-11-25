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

package cli_test

import (
	"bytes"
	"errors"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/cli"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/mocks"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/stretchr/testify/assert"
	"testing"
)

var columnNamesOut = `CLUSTER  ROLE     ADDRESS            STATISTICS
`
var primaryOut = `0        Primary  192.168.10.1:6379  UptimeInDays:1
                                     ConnectedClients:1
                                     ClientRecentMaxInputBuffer:2
                                     ClientRecentMaxOutputBuffer:0
                                     UsedMemory:2073528
                                     UsedMemoryHuman:1.98M
                                     UsedMemoryRss:13721600
                                     UsedMemoryRssHuman:13.09M
                                     UsedMemoryPeak:6706008
                                     UsedMemoryPeakHuman:6.40M
                                     UsedMemoryPeakPerc:30.92%
                                     MemFragmentationRatio:6.66
                                     MemFragmentationBytes:11379232
                                     TotalConnectionsReceived:566
                                     TotalCommandsProcessed:3635063
                                     SyncFull:2
                                     SyncPartialOk:21
                                     SyncPartialErr:0
                                     PubsubChannels:1
                                     UsedCpuSys:1457.472805
                                     UsedCpuUser:861.060018
                                     CmdstatReplconf:calls=1090183 usec:2790911 usecPerCall:2.56
                                     CmdstatKeys:calls=20 usec:233 usecPerCall:11.65
                                     CmdstatRole:calls=2 usec:84 usecPerCall:42.00
                                     CmdstatPsync:calls=23 usec:3582 usecPerCall:155.74
                                     CmdstatMset:calls=4 usec:30 usecPerCall:7.50
                                     CmdstatPublish:calls=804867 usec:12965499 usecPerCall:16.11
                                     CmdstatInfo:calls=164255 usec:57935065 usecPerCall:352.71
                                     CmdstatPing:calls=1582493 usec:3466844 usecPerCall:2.19
                                     CmdstatClient:calls=6 usec:11 usecPerCall:2.17
                                     CmdstatCommand:calls=482 usec:1462633 usecPerCall:3034.51
                                     CmdstatSubscribe:calls=3 usec:11 usecPerCall:3.67
                                     CmdstatMonitor:calls=3 usec:6 usecPerCall:2.00
                                     Db0:keys=123
`
var replicaOut = `0        Replica  192.168.10.2:6379  UptimeInDays:2
                                     ConnectedClients:8
                                     ClientRecentMaxInputBuffer:3
                                     ClientRecentMaxOutputBuffer:1
                                     UsedMemory:2135768
                                     UsedMemoryHuman:2.04M
                                     UsedMemoryRss:13721720
                                     UsedMemoryRssHuman:12.48M
                                     UsedMemoryPeak:6706009
                                     UsedMemoryPeakHuman:6.41M
                                     UsedMemoryPeakPerc:30.93%
                                     MemFragmentationRatio:6.67
                                     MemFragmentationBytes:11379233
                                     TotalConnectionsReceived:567
                                     TotalCommandsProcessed:3635064
                                     SyncFull:0
                                     SyncPartialOk:0
                                     SyncPartialErr:0
                                     PubsubChannels:1
                                     UsedCpuSys:1457.472806
                                     UsedCpuUser:861.060019
                                     CmdstatRole:calls=1 usec:3 usecPerCall:32.00
                                     CmdstatMset:calls=3 usec:14 usecPerCall:7.51
                                     CmdstatPublish:calls=804868 usec:12965498 usecPerCall:16.15
                                     CmdstatInfo:calls=164256 usec:57935066 usecPerCall:352.72
                                     CmdstatPing:calls=1582494 usec:3466845 usecPerCall:2.20
                                     CmdstatCommand:calls=483 usec:1462634 usecPerCall:3034.52
                                     CmdstatSubscribe:calls=3 usec:12 usecPerCall:3.68
                                     CmdstatMonitor:calls=6 usec:11 usecPerCall:2.11
                                     CmdstatConfig:calls=2 usec:1462 usecPerCall:146.74
                                     CmdstatSlaveof:calls=1 usec:412 usecPerCall:412.00
                                     Db0:keys=123
`

var statsMock *statisticsMock

type statisticsMock struct {
	dbIface      *mocks.MockDB
	dbErr        error
	dbStatistics sdlgoredis.DbStatistics
}

func newStatisticsDatabaseMock() *cli.Database {
	db := &cli.Database{}
	statsMock.dbIface = new(mocks.MockDB)
	statsMock.dbIface.On("Statistics").Return(&statsMock.dbStatistics, statsMock.dbErr)
	db.Instances = append(db.Instances, statsMock.dbIface)
	return db
}

func setupStatisticsMockDb() {
	if statsMock == nil {
		statsMock = new(statisticsMock)
	}
	statsMock.dbStatistics.Stats = []*sdlgoredis.DbStatisticsInfo{}
}

func setupStatisticsTestcase(tb testing.TB) func(tb testing.TB) {
	setupStatisticsMockDb()

	return func(tb testing.TB) {
		statsMock.dbStatistics.Stats = nil
	}
}

func setupStatisticsMockDbWithPrimaryInfo() {
	statsMock.dbStatistics.Stats = append(statsMock.dbStatistics.Stats,
		&sdlgoredis.DbStatisticsInfo{
			IPAddr: "192.168.10.1",
			Port:   "6379",
			Info: &sdlgoredis.DbInfo{
				Fields: sdlgoredis.DbInfoFields{
					PrimaryRole: true,
					Server: sdlgoredis.ServerInfoFields{
						UptimeInDays: 1,
					},
					Clients: sdlgoredis.ClientsInfoFields{
						ConnectedClients:            1,
						ClientRecentMaxInputBuffer:  2,
						ClientRecentMaxOutputBuffer: 0,
					},
					Memory: sdlgoredis.MeroryInfoFields{
						UsedMemory:            2073528,
						UsedMemoryHuman:       "1.98M",
						UsedMemoryRss:         13721600,
						UsedMemoryRssHuman:    "13.09M",
						UsedMemoryPeak:        6706008,
						UsedMemoryPeakHuman:   "6.40M",
						UsedMemoryPeakPerc:    "30.92%",
						MemFragmentationRatio: 6.66,
						MemFragmentationBytes: 11379232,
					},
					Stats: sdlgoredis.StatsInfoFields{
						TotalConnectionsReceived: 566,
						TotalCommandsProcessed:   3635063,
						SyncFull:                 2,
						SyncPartialOk:            21,
						SyncPartialErr:           0,
						PubsubChannels:           1,
					},
					Cpu: sdlgoredis.CpuInfoFields{
						UsedCpuSys:  1457.472805,
						UsedCpuUser: 861.060018,
					},
					Commandstats: sdlgoredis.CommandstatsInfoFields{
						CmdstatReplconf: sdlgoredis.CommandstatsValues{
							Calls:       1090183,
							Usec:        2790911,
							UsecPerCall: 2.56,
						},
						CmdstatKeys: sdlgoredis.CommandstatsValues{
							Calls:       20,
							Usec:        233,
							UsecPerCall: 11.65,
						},
						CmdstatRole: sdlgoredis.CommandstatsValues{
							Calls:       2,
							Usec:        84,
							UsecPerCall: 42.00,
						},
						CmdstatConfig: sdlgoredis.CommandstatsValues{},
						CmdstatPsync: sdlgoredis.CommandstatsValues{
							Calls:       23,
							Usec:        3582,
							UsecPerCall: 155.74,
						},
						CmdstatMset: sdlgoredis.CommandstatsValues{
							Calls:       4,
							Usec:        30,
							UsecPerCall: 7.50,
						},
						CmdstatPublish: sdlgoredis.CommandstatsValues{
							Calls:       804867,
							Usec:        12965499,
							UsecPerCall: 16.11,
						},
						CmdstatInfo: sdlgoredis.CommandstatsValues{
							Calls:       164255,
							Usec:        57935065,
							UsecPerCall: 352.71,
						},
						CmdstatPing: sdlgoredis.CommandstatsValues{
							Calls:       1582493,
							Usec:        3466844,
							UsecPerCall: 2.19,
						},
						CmdstatClient: sdlgoredis.CommandstatsValues{
							Calls:       6,
							Usec:        11,
							UsecPerCall: 2.17,
						},
						CmdstatCommand: sdlgoredis.CommandstatsValues{
							Calls:       482,
							Usec:        1462633,
							UsecPerCall: 3034.51,
						},
						CmdstatSubscribe: sdlgoredis.CommandstatsValues{
							Calls:       3,
							Usec:        11,
							UsecPerCall: 3.67,
						},
						CmdstatMonitor: sdlgoredis.CommandstatsValues{
							Calls:       3,
							Usec:        6,
							UsecPerCall: 2.00,
						},
						CmdstatSlaveof: sdlgoredis.CommandstatsValues{},
					},
					Keyspace: sdlgoredis.KeyspaceInfoFields{
						Db: sdlgoredis.KeyspaceValues{
							Keys: 123,
						},
					},
				},
			},
		},
	)
}

func setupStatisticsMockDbWithReplicaInfo() {
	statsMock.dbStatistics.Stats = append(statsMock.dbStatistics.Stats,
		&sdlgoredis.DbStatisticsInfo{
			IPAddr: "192.168.10.2",
			Port:   "6379",
			Info: &sdlgoredis.DbInfo{
				Fields: sdlgoredis.DbInfoFields{
					PrimaryRole: false,
					Server: sdlgoredis.ServerInfoFields{
						UptimeInDays: 2,
					},
					Clients: sdlgoredis.ClientsInfoFields{
						ConnectedClients:            8,
						ClientRecentMaxInputBuffer:  3,
						ClientRecentMaxOutputBuffer: 1,
					},
					Memory: sdlgoredis.MeroryInfoFields{
						UsedMemory:            2135768,
						UsedMemoryHuman:       "2.04M",
						UsedMemoryRss:         13721720,
						UsedMemoryRssHuman:    "12.48M",
						UsedMemoryPeak:        6706009,
						UsedMemoryPeakHuman:   "6.41M",
						UsedMemoryPeakPerc:    "30.93%",
						MemFragmentationRatio: 6.67,
						MemFragmentationBytes: 11379233,
					},
					Stats: sdlgoredis.StatsInfoFields{
						TotalConnectionsReceived: 567,
						TotalCommandsProcessed:   3635064,
						SyncFull:                 0,
						SyncPartialOk:            0,
						SyncPartialErr:           0,
						PubsubChannels:           1,
					},
					Cpu: sdlgoredis.CpuInfoFields{
						UsedCpuSys:  1457.472806,
						UsedCpuUser: 861.060019,
					},
					Commandstats: sdlgoredis.CommandstatsInfoFields{
						CmdstatReplconf: sdlgoredis.CommandstatsValues{},
						CmdstatKeys:     sdlgoredis.CommandstatsValues{},
						CmdstatRole: sdlgoredis.CommandstatsValues{
							Calls:       1,
							Usec:        3,
							UsecPerCall: 32.00,
						},
						CmdstatConfig: sdlgoredis.CommandstatsValues{
							Calls:       2,
							Usec:        1462,
							UsecPerCall: 146.74,
						},
						CmdstatPsync: sdlgoredis.CommandstatsValues{},
						CmdstatMset: sdlgoredis.CommandstatsValues{
							Calls:       3,
							Usec:        14,
							UsecPerCall: 7.51,
						},
						CmdstatPublish: sdlgoredis.CommandstatsValues{
							Calls:       804868,
							Usec:        12965498,
							UsecPerCall: 16.15,
						},
						CmdstatInfo: sdlgoredis.CommandstatsValues{
							Calls:       164256,
							Usec:        57935066,
							UsecPerCall: 352.72,
						},
						CmdstatPing: sdlgoredis.CommandstatsValues{
							Calls:       1582494,
							Usec:        3466845,
							UsecPerCall: 2.20,
						},
						CmdstatClient: sdlgoredis.CommandstatsValues{},
						CmdstatCommand: sdlgoredis.CommandstatsValues{
							Calls:       483,
							Usec:        1462634,
							UsecPerCall: 3034.52,
						},
						CmdstatSubscribe: sdlgoredis.CommandstatsValues{
							Calls:       3,
							Usec:        12,
							UsecPerCall: 3.68,
						},
						CmdstatMonitor: sdlgoredis.CommandstatsValues{
							Calls:       6,
							Usec:        11,
							UsecPerCall: 2.11,
						},
						CmdstatSlaveof: sdlgoredis.CommandstatsValues{
							Calls:       1,
							Usec:        412,
							UsecPerCall: 412.00,
						},
					},
					Keyspace: sdlgoredis.KeyspaceInfoFields{
						Db: sdlgoredis.KeyspaceValues{
							Keys: 123,
						},
					},
				},
			},
		},
	)
}

func runStatisticsCli() (string, error) {
	buf := new(bytes.Buffer)
	cmd := cli.NewStatisticsCmd(newStatisticsDatabaseMock)
	cmd.SetOut(buf)

	err := cmd.Execute()

	return buf.String(), err
}

func TestCliStatisticsCanShowHelp(t *testing.T) {
	var expOkErr error
	expHelp := "Usage:\n  " + "statistics [flags]"
	expNokErr := errors.New("unknown flag: --some-unknown-flag")
	expArgCntErr := errors.New("accepts 0 arg(s), received 1")
	tests := []struct {
		args      string
		expErr    error
		expOutput string
	}{
		{args: "-h", expErr: expOkErr, expOutput: expHelp},
		{args: "--help", expErr: expOkErr, expOutput: expHelp},
		{args: "--some-unknown-flag", expErr: expNokErr, expOutput: expHelp},
		{args: "some-extra-argument", expErr: expArgCntErr, expOutput: expHelp},
	}

	for _, test := range tests {
		buf := new(bytes.Buffer)
		cmd := cli.NewStatisticsCmd(newStatisticsDatabaseMock)
		cmd.SetOut(buf)
		cmd.SetArgs([]string{test.args})

		err := cmd.Execute()

		stdout := buf.String()
		assert.Equal(t, test.expErr, err)
		assert.Contains(t, stdout, test.expOutput)
	}
}

func TestCliStatisticsShowOnlyColumnsNames(t *testing.T) {
	teardownTest := setupStatisticsTestcase(t)
	defer teardownTest(t)

	expOut := "CLUSTER  ROLE  ADDRESS  STATISTICS\n"

	stdout, err := runStatisticsCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliStatisticsShowPrimaryInfo(t *testing.T) {
	teardownTest := setupStatisticsTestcase(t)
	defer teardownTest(t)

	setupStatisticsMockDbWithPrimaryInfo()
	expOut := columnNamesOut + primaryOut

	stdout, err := runStatisticsCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliStatisticsShowPrimaryAndReplicaInfo(t *testing.T) {
	teardownTest := setupStatisticsTestcase(t)
	defer teardownTest(t)

	setupStatisticsMockDbWithPrimaryInfo()
	setupStatisticsMockDbWithReplicaInfo()
	expOut := columnNamesOut + primaryOut + replicaOut

	stdout, err := runStatisticsCli()

	assert.Nil(t, err)
	assert.Equal(t, expOut, stdout)
}

func TestCliStatisticsRaisesError(t *testing.T) {
	teardownTest := setupStatisticsTestcase(t)
	defer teardownTest(t)

	expErr := errors.New("Boom!")
	expOut := "Usage:\n  statistics [flags]"
	statsMock.dbErr = errors.New("Boom!")

	stdout, err := runStatisticsCli()

	assert.Equal(t, expErr, err)
	assert.Contains(t, stdout, expOut)
}
