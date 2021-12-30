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

package cli

import (
	"fmt"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/spf13/cobra"
	"os"
	"reflect"
	"text/tabwriter"
)

func init() {
	rootCmd.AddCommand(newStatisticsCmd(newDatabase))
}

var (
	statsLong = `Display SDL resource usage statistics`

	statsExample = `  # Show statistics per node
  sdlcli statistics`
)

func newStatisticsCmd(dbCreateCb DbCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "statistics",
		Short:   "Display statistics.",
		Long:    statsLong,
		Example: statsExample,
		Args:    cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			statistics, err := runStats(dbCreateCb)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", buf.String())
				return err
			}
			printStatistics(cmd, statistics)
			return nil
		},
	}
	cmd.SetOut(os.Stdout)
	return cmd
}

func runStats(dbCreateCb DbCreateCb) ([]*sdlgoredis.DbStatistics, error) {
	var statistics []*sdlgoredis.DbStatistics
	for _, dbInst := range dbCreateCb().Instances {
		dbStatistics, err := dbInst.Statistics()
		if err != nil {
			return nil, err
		}
		statistics = append(statistics, dbStatistics)
	}
	return statistics, nil
}

func writeClientsInfo(w *tabwriter.Writer, clients sdlgoredis.ClientsInfoFields) {
	fmt.Fprintf(w, "\t\t\tConnectedClients:%d\n", clients.ConnectedClients)
	fmt.Fprintf(w, "\t\t\tClientRecentMaxInputBuffer:%d\n", clients.ClientRecentMaxInputBuffer)
	fmt.Fprintf(w, "\t\t\tClientRecentMaxOutputBuffer:%d\n", clients.ClientRecentMaxOutputBuffer)
}

func writeMemoryInfo(w *tabwriter.Writer, memory sdlgoredis.MeroryInfoFields) {
	fmt.Fprintf(w, "\t\t\tUsedMemory:%d\n", memory.UsedMemory)
	fmt.Fprintf(w, "\t\t\tUsedMemoryHuman:%s\n", memory.UsedMemoryHuman)
	fmt.Fprintf(w, "\t\t\tUsedMemoryRss:%d\n", memory.UsedMemoryRss)
	fmt.Fprintf(w, "\t\t\tUsedMemoryRssHuman:%s\n", memory.UsedMemoryRssHuman)
	fmt.Fprintf(w, "\t\t\tUsedMemoryPeak:%d\n", memory.UsedMemoryPeak)
	fmt.Fprintf(w, "\t\t\tUsedMemoryPeakHuman:%s\n", memory.UsedMemoryPeakHuman)
	fmt.Fprintf(w, "\t\t\tUsedMemoryPeakPerc:%s\n", memory.UsedMemoryPeakPerc)
	fmt.Fprintf(w, "\t\t\tMemFragmentationRatio:%.2f\n", memory.MemFragmentationRatio)
	fmt.Fprintf(w, "\t\t\tMemFragmentationBytes:%d\n", memory.MemFragmentationBytes)
}

func writeStatsInfo(w *tabwriter.Writer, stats sdlgoredis.StatsInfoFields) {
	fmt.Fprintf(w, "\t\t\tTotalConnectionsReceived:%d\n", stats.TotalConnectionsReceived)
	fmt.Fprintf(w, "\t\t\tTotalCommandsProcessed:%d\n", stats.TotalCommandsProcessed)
	fmt.Fprintf(w, "\t\t\tSyncFull:%d\n", stats.SyncFull)
	fmt.Fprintf(w, "\t\t\tSyncPartialOk:%d\n", stats.SyncPartialOk)
	fmt.Fprintf(w, "\t\t\tSyncPartialErr:%d\n", stats.SyncPartialErr)
	fmt.Fprintf(w, "\t\t\tPubsubChannels:%d\n", stats.PubsubChannels)
}

func writeCpuInfo(w *tabwriter.Writer, cpu sdlgoredis.CpuInfoFields) {
	fmt.Fprintf(w, "\t\t\tUsedCpuSys:%v\n", cpu.UsedCpuSys)
	fmt.Fprintf(w, "\t\t\tUsedCpuUser:%v\n", cpu.UsedCpuUser)
}

func fillCommandstatsInfo(w *tabwriter.Writer, i interface{}, cmdstat string) {
	stype := reflect.ValueOf(i).Elem()
	callsField := stype.FieldByName("Calls").Interface()
	usecField := stype.FieldByName("Usec").Interface()
	usecPerCallField := stype.FieldByName("UsecPerCall").Interface()

	if callsField.(uint32) > 0 {
		fmt.Fprintf(w, "\t\t\t%s:calls=%d usec:%d usecPerCall:%.2f\n",
			cmdstat, callsField, usecField, usecPerCallField)
	}
}

func writeCommandstatsInfo(w *tabwriter.Writer, commandstats sdlgoredis.CommandstatsInfoFields) {
	fillCommandstatsInfo(w, &commandstats.CmdstatReplconf, "CmdstatReplconf")
	fillCommandstatsInfo(w, &commandstats.CmdstatKeys, "CmdstatKeys")
	fillCommandstatsInfo(w, &commandstats.CmdstatRole, "CmdstatRole")
	fillCommandstatsInfo(w, &commandstats.CmdstatPsync, "CmdstatPsync")
	fillCommandstatsInfo(w, &commandstats.CmdstatMset, "CmdstatMset")
	fillCommandstatsInfo(w, &commandstats.CmdstatPublish, "CmdstatPublish")
	fillCommandstatsInfo(w, &commandstats.CmdstatInfo, "CmdstatInfo")
	fillCommandstatsInfo(w, &commandstats.CmdstatPing, "CmdstatPing")
	fillCommandstatsInfo(w, &commandstats.CmdstatClient, "CmdstatClient")
	fillCommandstatsInfo(w, &commandstats.CmdstatCommand, "CmdstatCommand")
	fillCommandstatsInfo(w, &commandstats.CmdstatSubscribe, "CmdstatSubscribe")
	fillCommandstatsInfo(w, &commandstats.CmdstatMonitor, "CmdstatMonitor")
	fillCommandstatsInfo(w, &commandstats.CmdstatConfig, "CmdstatConfig")
	fillCommandstatsInfo(w, &commandstats.CmdstatSlaveof, "CmdstatSlaveof")
}

func writeKeyspaceInfo(w *tabwriter.Writer, keyspace sdlgoredis.KeyspaceInfoFields) {
	fmt.Fprintf(w, "\t\t\tDb0:keys=%d\n", keyspace.Db.Keys)
}

func printStatistics(cmd *cobra.Command, statistics []*sdlgoredis.DbStatistics) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 6, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tROLE\tADDRESS\tSTATISTICS")

	for i, s := range statistics {
		for _, stats := range s.Stats {
			fmt.Fprintf(w, "%d", i)
			if stats.Info.Fields.PrimaryRole {
				fmt.Fprintf(w, "\tPrimary")
			} else {
				fmt.Fprintf(w, "\tReplica")
			}
			fmt.Fprintf(w, "\t%s:%s", stats.IPAddr, stats.Port)
			fmt.Fprintf(w, "\tUptimeInDays:%d\n", stats.Info.Fields.Server.UptimeInDays)
			writeClientsInfo(w, stats.Info.Fields.Clients)
			writeMemoryInfo(w, stats.Info.Fields.Memory)
			writeStatsInfo(w, stats.Info.Fields.Stats)
			writeCpuInfo(w, stats.Info.Fields.Cpu)
			writeCommandstatsInfo(w, stats.Info.Fields.Commandstats)
			writeKeyspaceInfo(w, stats.Info.Fields.Keyspace)
		}
	}
	w.Flush()
}
