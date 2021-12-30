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
	"os"
	"sort"

	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/spf13/cobra"
)

var getCmd = newGetCmd(func() ISyncStorage {
	return sdlgo.NewSyncStorage()
})

func init() {
	rootCmd.AddCommand(getCmd)
}

var (
	getLong = `Display one or many resources.

Prints namespaces, keys or keys data in the given namespace.`

	getExample = `  # List all the namespaces in database.
  sdlcli get namespaces

  # List keys in the given namespace.
  sdlcli get keys sdlns

  # Reads key data in the given namespace.
  sdlcli get sdlns key1

  # Read multiple keys data in the given namespace.
  sdlcli get sdlns key1 key2 key3`
)

func newGetCmd(sdlCb SyncStorageCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get <namespace> <key> [<key2> <key3>... <keyN>]",
		Short:   "Display one or many resources",
		Long:    getLong,
		Example: getExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			sdlgoredis.SetDbLogger(&buf)
			if len(args) < 2 {
				return fmt.Errorf("accepts command or arguments, received %d", len(args))
			}
			data, err := runGet(sdlCb, args)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", buf.String())
				return err
			}
			printData(cmd, data)
			return nil
		},
	}
	cmd.SetOut(os.Stdout)
	return cmd
}

func runGet(sdlCb SyncStorageCreateCb, args []string) (map[string]interface{}, error) {
	data, err := sdlCb().Get(args[0], args[1:])
	if err != nil {
		return nil, err
	}
	return data, nil
}

func printData(cmd *cobra.Command, data map[string]interface{}) {
	var str string
	var sortedKeys []string
	for key := range data {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		value, ok := data[k]
		if ok && value != nil {
			str = fmt.Sprintf("%s:%s", k, value)
			cmd.Printf(str + "\n")
		}
	}
}
