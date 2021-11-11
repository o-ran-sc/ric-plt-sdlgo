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
)

func init() {
	rootCmd.AddCommand(newHealthCheckCmd(newDatabase))
}

func newHealthCheckCmd(dbCreateCb DbCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Validate database healthiness",
		Long:  `Validate database healthiness`,
		RunE: func(cmd *cobra.Command, args []string) error {
			out, err := runHealthCheck(dbCreateCb)
			cmd.Println(out)
			if err != nil {
				cmd.PrintErrf("%s\n", buf.String())
			}
			return err
		},
	}
	cmd.SetOut(os.Stdout)
	return cmd
}

func runHealthCheck(dbCreateCb DbCreateCb) (string, error) {
	var anyErr error
	var str string
	var states []sdlgoredis.DbState
	for _, dbInst := range dbCreateCb().Instances {
		info, err := dbInst.State()
		if err != nil {
			anyErr = err
		}
		states = append(states, *info)
	}
	str = writeStateResults(states)
	return str, anyErr
}

func writeStateResults(dbStates []sdlgoredis.DbState) string {
	var str string
	var anyErr error
	for i, dbState := range dbStates {
		if err := dbState.IsOnline(); err != nil {
			anyErr = err
		}
		str = str + fmt.Sprintf("  SDL DB backend #%d\n", (i+1))
		mAddr := dbState.MasterDbState.GetAddress()
		err := dbState.MasterDbState.IsOnline()
		if err == nil {
			str = str + fmt.Sprintf("    Master (%s): OK\n", mAddr)
		} else {
			str = str + fmt.Sprintf("    Master (%s): NOK\n", mAddr)
			str = str + fmt.Sprintf("      %s\n", err.Error())
		}
		if dbState.ReplicasDbState != nil {
			for j, rInfo := range dbState.ReplicasDbState.States {
				err := rInfo.IsOnline()
				if err == nil {
					str = str + fmt.Sprintf("    Replica #%d (%s): OK\n", (j+1), rInfo.GetAddress())
				} else {
					str = str + fmt.Sprintf("    Replica #%d (%s): NOK\n", (j+1), rInfo.GetAddress())
					str = str + fmt.Sprintf("      %s\n", err.Error())
				}
			}
		}
		if dbState.SentinelsDbState != nil {
			for k, sInfo := range dbState.SentinelsDbState.States {
				err := sInfo.IsOnline()
				if err != nil {
					str = str + fmt.Sprintf("    Sentinel #%d (%s): NOK\n", (k+1), sInfo.GetAddress())
					str = str + fmt.Sprintf("      %s\n", err.Error())
				}
			}
		}
	}
	if anyErr == nil {
		str = fmt.Sprintf("Overall status: OK\n\n") + str
	} else {
		str = fmt.Sprintf("Overall status: NOK\n\n") + str
	}
	return str
}
