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

func init() {
	getCmd.AddCommand(newKeysCmd(func() ISyncStorage {
		return sdlgo.NewSyncStorage()
	}))
}

var (
	keysLong = `List keys in the given namespace matching key search pattern.`

	keysExample = `  # List all keys in the given namespace.
  sdlcli get keys sdlns
  # List keys in the given namespace matching given key search pattern.
  sdlcli get keys sdlns 'he*'

  Supported search glob-style patterns:
    h?llo matches hello, hallo and hxllo
    h*llo matches hllo and heeeello
    h[ae]llo matches hello and hallo, but not hillo
    h[^e]llo matches hallo, hbllo, ... but not hello
    h[a-b]llo matches hallo and hbllo

  The \ escapes character in key search pattern and those will be treated as a normal
  character:
    h\[?llo\* matches h[ello* and h[allo*`
)

func newKeysCmd(sdlCb SyncStorageCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "keys <namespace> [pattern|default '*']",
		Short:   "List keys in the given namespace matching key search pattern",
		Long:    keysLong,
		Example: keysExample,
		Args:    cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sdlgoredis.SetDbLogger(&buf)
			keysArgs := NewKeysArgs(args[0], "*")
			if len(args) > 1 {
				keysArgs.pattern = args[1]
			}
			if err := keysArgs.Validate(); err != nil {
				return err
			}
			keys, err := runListKeys(sdlCb, keysArgs)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s", buf.String())
				return err
			}
			printKeys(cmd, keys)
			return nil
		},
	}
	cmd.SetOut(os.Stdout)
	return cmd
}

func runListKeys(sdlCb SyncStorageCreateCb, args keysArgs) ([]string, error) {
	keys, err := sdlCb().ListKeys(args.ns, args.pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, err
}

func printKeys(cmd *cobra.Command, keys []string) {
	for _, k := range keys {
		cmd.Println(k)
	}
}
