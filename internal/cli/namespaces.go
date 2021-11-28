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
	"text/tabwriter"
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
			showWide, _ := cmd.Flags().GetBool("wide")
			nsMap, err := runNamespaces(dbCreateCb)
			if err != nil {
				cmd.PrintErrf("%s\n", buf.String())
				return err
			}
			if showWide {
				printNamespacesWide(cmd, nsMap)
			} else {
				printNamespaces(cmd, nsMap)
			}
			return err
		},
	}
	cmd.SetOut(os.Stdout)
	cmd.Flags().BoolP("wide", "w", false, "Show SDL DB cluster address, namespace name and its keys count")
	return cmd
}

func runNamespaces(dbCreateCb DbCreateCb) (nsMap, error) {
	nsMap := make(nsMap)
	for _, dbInst := range dbCreateCb().Instances {
		keys, err := dbInst.Keys("*")
		if err != nil {
			return nil, err
		}
		id, err := getServiceAddress(dbInst)
		if err != nil {
			return nil, err
		}

		if _, ok := nsMap[id]; !ok {
			nsMap[id] = make(nsKeyMap)
		}

		for _, key := range keys {
			namespace, err := parseKeyNamespace(key)
			if err != nil {
				return nil, err
			}
			if isUniqueNamespace(nsMap[id], namespace) {
				nsMap[id][namespace] = 1
			} else {
				nsMap[id][namespace]++
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
	return state.PrimaryDbState.GetAddress(), nil
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

func isUniqueNamespace(namespaces nsKeyMap, newNs string) bool {
	if _, ok := namespaces[newNs]; ok {
		return false
	}
	return true
}

func printNamespaces(cmd *cobra.Command, nsMap nsMap) {
	var nsList []string
	for _, nsKeyMap := range nsMap {
		for ns, _ := range nsKeyMap {
			nsList = append(nsList, ns)
		}
	}

	sort.Strings(nsList)
	for _, ns := range nsList {
		cmd.Println(ns)
	}
}

func printNamespacesWide(cmd *cobra.Command, nsMap nsMap) {
	var nsList []string
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 6, 4, 3, ' ', 0)
	fmt.Fprintln(w, "ADDRESS\tNAMESPACE\tKEYS\t")
	for addr, nsKeyMap := range nsMap {
		for ns, _ := range nsKeyMap {
			nsList = append(nsList, ns)
		}
		sort.Strings(nsList)
		for _, ns := range nsList {
			if addr == "" {
				fmt.Fprintf(w, "\t%s\t%d\t\n", ns, nsKeyMap[ns])
			} else {
				fmt.Fprintf(w, "%s\t%s\t%d\t\n", addr, ns, nsKeyMap[ns])
			}
		}
		nsList = nil
	}
	w.Flush()
}
