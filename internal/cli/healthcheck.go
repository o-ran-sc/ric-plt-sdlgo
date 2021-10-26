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
	"bytes"
	"fmt"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func NewHealthCheckCmd() *cobra.Command {
	return newHealthCheckCmd(newDatabase)
}

func newHealthCheckCmd(dbCreator DbCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "healthcheck",
		Long:  `healthcheck command validates connection towards database backend`,
		RunE: func(cmd *cobra.Command, args []string) error {
			var buf bytes.Buffer
			sdlgoredis.SetDbLogger(&buf)
			out, err := runHealthcheck(dbCreator)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", buf.String())
			}
			cmd.Println(out)
			return err
		},
	}
	return cmd
}

func runHealthcheck(dbCreator DbCreateCb) (string, error) {
	var infos []*internal.DbInfo
	for _, dbInst := range dbCreator().Instances {
		info, err := dbInst.Info()
		if err != nil {
			return "", fmt.Errorf("SDL API error: %v", err)
		}
		infos = append(infos, info)
	}
	str := write_results(infos)
	return str, nil
}

func write_results(infos []*internal.DbInfo) string {
	var anyFailure bool
	var allInfoStr string
	for i, info := range infos {
		failureStatus, resStr := process_info_result(i+1, info)
		if failureStatus == true {
			anyFailure = true
		}
		allInfoStr += resStr
	}
	var str string
	if anyFailure == false {
		str = fmt.Sprintf("Overall status: OK\n\n")
	} else {
		str = fmt.Sprintf("Overall status: NOK\n\n")
	}
	return str + fmt.Sprintln(allInfoStr)
}

func process_info_result(infoNum int, info *internal.DbInfo) (bool, string) {
	var anyFailure bool
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("  SDL DB backend #%d\n", infoNum))
	if info.MasterRole == true {
		if uint32(len(info.Replicas)) < info.ConfReplicasCnt {
			sb.WriteString(fmt.Sprintf("    Connected replicas count %d: NOK\n", len(info.Replicas)))
			sb.WriteString(fmt.Sprintf("      expected: %d\n", info.ConfReplicasCnt))
			anyFailure = true
		} else {
			sb.WriteString(fmt.Sprintf("    Connected replicas count %d: OK\n", len(info.Replicas)))
			for i, replica := range info.Replicas {
				if replica.State != "online" {
					sb.WriteString(fmt.Sprintf("    Replica #%d (%s): NOK\n", i+1, replica.Addr))
					sb.WriteString(fmt.Sprintf("      State: %s\n", replica.State))
					anyFailure = true
				} else {
					sb.WriteString(fmt.Sprintf("    Replica #%d (%s): OK\n", i+1, replica.Addr))
				}
			}
		}
	} else {
		sb.WriteString(fmt.Sprintln("    Connected to master: NOK"))
		anyFailure = true
	}
	return anyFailure, sb.String()
}
