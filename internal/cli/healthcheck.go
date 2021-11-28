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
	"text/tabwriter"
)

func init() {
	rootCmd.AddCommand(newHealthCheckCmd(newDatabase))
}

func newHealthCheckCmd(dbCreateCb DbCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Validate SDL database healthiness",
		Long:  `Validate SDL database healthiness`,
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			states, err := runHealthCheck(dbCreateCb)
			printHealthStatus(cmd, states)
			return err
		},
	}
	cmd.SetOut(os.Stdout)
	return cmd
}

func runHealthCheck(dbCreateCb DbCreateCb) ([]sdlgoredis.DbState, error) {
	var anyErr error
	var states []sdlgoredis.DbState
	for _, dbInst := range dbCreateCb().Instances {
		state, err := dbInst.State()
		if err != nil {
			anyErr = err
		}
		states = append(states, *state)
	}
	return states, anyErr
}

func printHealthStatus(cmd *cobra.Command, dbStates []sdlgoredis.DbState) {
	var anyErr error
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 6, 4, 3, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tROLE\tADDRESS\tSTATUS\tERROR\t")

	for i, dbState := range dbStates {
		if err := dbState.IsOnline(); err != nil {
			anyErr = err
		}

		if err := printPrimaryHealthStatus(w, i, &dbState); err != nil {
			anyErr = err
		}

		if err := printReplicasHealthStatus(w, i, &dbState); err != nil {
			anyErr = err
		}

		if err := printSentinelsHealthStatus(w, i, &dbState); err != nil {
			anyErr = err
		}
	}
	if anyErr == nil {
		cmd.Println("Overall status: OK")
	} else {
		cmd.Println("Overall status: NOK")
	}
	cmd.Println("")
	w.Flush()
}

func printPrimaryHealthStatus(w *tabwriter.Writer, clusterID int, dbState *sdlgoredis.DbState) error {
	addr := printAddress(dbState.PrimaryDbState.GetAddress())
	err := dbState.PrimaryDbState.IsOnline()
	if err != nil {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t\n", clusterID, "primary", addr, "NOK", err.Error())
	} else {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t\n", clusterID, "primary", addr, "OK", "<none>")
	}
	return err
}

func printReplicasHealthStatus(w *tabwriter.Writer, clusterID int, dbState *sdlgoredis.DbState) error {
	var anyErr error

	if dbState.ReplicasDbState != nil {
		if dbState.ConfigNodeCnt > len(dbState.ReplicasDbState.States)+1 {
			err := fmt.Errorf("Configured DBAAS nodes %d but only 1 primary and %d replicas",
				dbState.ConfigNodeCnt, len(dbState.ReplicasDbState.States))
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t\n", clusterID, "replica", "<none>", "NOK", err.Error())
			anyErr = err
		}
		for _, state := range dbState.ReplicasDbState.States {
			if err := printReplicaHealthStatus(w, clusterID, state); err != nil {
				anyErr = err
			}
		}
	}
	return anyErr
}

func printReplicaHealthStatus(w *tabwriter.Writer, clusterID int, dbState *sdlgoredis.ReplicaDbState) error {
	addr := printAddress(dbState.GetAddress())
	err := dbState.IsOnline()
	if err != nil {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t\n", clusterID, "replica", addr, "NOK", err.Error())
	} else {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t\n", clusterID, "replica", addr, "OK", "<none>")
	}
	return err
}

func printSentinelsHealthStatus(w *tabwriter.Writer, clusterID int, dbState *sdlgoredis.DbState) error {
	var anyErr error
	if dbState.SentinelsDbState != nil {
		for _, state := range dbState.SentinelsDbState.States {
			if err := printSentinelHealthStatus(w, clusterID, state); err != nil {
				anyErr = err
			}
		}
	}
	return anyErr
}
func printSentinelHealthStatus(w *tabwriter.Writer, clusterID int, dbState *sdlgoredis.SentinelDbState) error {
	err := dbState.IsOnline()
	if err != nil {
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t\n", clusterID, "sentinel", dbState.GetAddress(), "NOK", err.Error())
	}
	return err
}

func printAddress(address string) string {
	if address == "" {
		return "<none>"
	}
	return address
}
