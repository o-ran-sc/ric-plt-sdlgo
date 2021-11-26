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
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/spf13/cobra"
	"os"
)

const removeLongHelp = "Remove key(s) under given namespace from SDL DB. A key to remove\n" +
	"can be an exact key name or a key search pattern. Multiple key names\n" +
	"or search patterns can be given in a single command, each of which\n" +
	"is separated from the one next to it by a space. If no key or\n" +
	"search pattern is given, command removes all the keys under given\n" +
	"namespace."

const removeExample = "  # Remove all keys in the given namespace.\n" +
	"  sdlcli remove sdlns\n" +
	"  # Remove keys 'key1' and 'key2' in the given namespace.\n" +
	"  sdlcli remove sdlns key1 key2\n" +
	"  # Remove keys in the given namespace matching given key search pattern.\n" +
	"  sdlcli remove sdlns 'he*'\n\n" +

	"  Supported search glob-style patterns:\n" +
	"    h?llo matches hello, hallo and hxllo\n" +
	"    h*llo matches hllo and heeeello\n" +
	"    h[ae]llo matches hello and hallo, but not hillo\n" +
	"    h[^e]llo matches hallo, hbllo, ... but not hello\n" +
	"    h[a-b]llo matches hallo and hbllo\n\n" +
	"  The \\ escapes character in key search pattern and those will be\n" +
	"  treated as a normal character:\n" +
	"    h\\[?llo\\* matches h[ello* and h[allo"

func init() {
	rootCmd.AddCommand(newRemoveCmd(func() ISyncStorage {
		return sdlgo.NewSyncStorage()
	}))
}

func newRemoveCmd(sdlCreateCb SyncStorageCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remove <namespace> [<key|pattern>... <keyN|patternN>]",
		Short:   "Remove key(s) under given namespace from SDL DB",
		Long:    removeLongHelp,
		Example: removeExample,
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var buf bytes.Buffer
			sdlgoredis.SetDbLogger(&buf)
			err := runRemove(sdlCreateCb, args)
			if err != nil {
				cmd.PrintErrf("%s\n", buf.String())
			}
			return err
		},
	}
	cmd.SetOut(os.Stdout)
	return cmd
}

func runRemove(sdlCreateCb SyncStorageCreateCb, args []string) error {
	sdl := sdlCreateCb()
	ns := args[0]
	keyPatterns := []string{"*"}
	if len(args) > 1 {
		keyPatterns = args[1:]
	}
	for _, keyPattern := range keyPatterns {
		keys, err := sdl.ListKeys(ns, keyPattern)
		if err != nil {
			return err
		}
		if err := sdl.Remove(ns, keys); err != nil {
			return err
		}
	}
	return nil
}
