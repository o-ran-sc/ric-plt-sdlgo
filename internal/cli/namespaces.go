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
	"github.com/spf13/cobra"
	"os"
	"sort"
	"strings"
)

func init() {
	getCmd.AddCommand(newNamespacesCmd(newDatabase))
}

func newNamespacesCmd(dbCreateCb DbCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "namespaces",
		Short: "List all the namespaces in database",
		Long:  "List all the namespaces in database",
		Args:  cobra.ExactArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			showPerDb, _ := cmd.Flags().GetBool("group")
			nsMap, err := runNamespaces(dbCreateCb)
			if err != nil {
				cmd.PrintErrf("%s\n", buf.String())
				return err
			}
			if showPerDb {
				printNamespacesPerDb(cmd, nsMap)
			} else {
				printNamespaces(cmd, nsMap)
			}
			return err
		},
	}
	cmd.SetOut(os.Stdout)
	cmd.Flags().BoolP("group", "g", false, "Show namespaces per SDL DB cluster group")
	return cmd
}

func runNamespaces(dbCreateCb DbCreateCb) (map[string][]string, error) {
	nsMap := make(map[string][]string)
	for _, dbInst := range dbCreateCb().Instances {
		keys, err := dbInst.Keys("*")
		if err != nil {
			return nil, err
		}
		id, err := getServiceAddress(dbInst)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			namespace, err := parseKeyNamespace(key)
			if err != nil {
				return nil, err
			}
			if isUniqueNamespace(nsMap[id], namespace) {
				nsMap[id] = append(nsMap[id], namespace)
			}
		}
	}
	return nsMap, nil
}

func getServiceAddress(db iDatabase) (string, error) {
	state, err := db.State()
	if err != nil {
		return "", err
	}
	return state.MasterDbState.GetAddress(), nil
}

func parseKeyNamespace(key string) (string, error) {
	sIndex := strings.Index(key, "{")
	if sIndex == -1 {
		return "", fmt.Errorf("Namespace parsing error, no '{' in key string '%s'", key)
	}
	str := key[sIndex+len("{"):]
	eIndex := strings.Index(str, "}")
	if eIndex == -1 {
		return "", fmt.Errorf("Namespace parsing error, no '}' in key string '%s'", key)
	}
	return str[:eIndex], nil
}

func isUniqueNamespace(namespaces []string, newNs string) bool {
	for _, n := range namespaces {
		if n == newNs {
			return false
		}
	}
	return true
}

func printNamespaces(cmd *cobra.Command, nsMap map[string][]string) {
	var namespaces []string
	for _, groupNss := range nsMap {
		namespaces = append(namespaces, groupNss...)
	}

	sort.Strings(namespaces)
	for _, n := range namespaces {
		cmd.Println(n)
	}
}

func printNamespacesPerDb(cmd *cobra.Command, nsMap map[string][]string) {
	for addr, groupNss := range nsMap {
		sort.Strings(groupNss)
		for _, ns := range groupNss {
			if addr == "" {
				cmd.Printf("%s\n", ns)
			} else {
				cmd.Printf("%s: %s\n", addr, ns)
			}
		}
	}
}
