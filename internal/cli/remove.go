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

func init() {
	rootCmd.AddCommand(newRemoveCmd(func() ISyncStorage {
		return sdlgo.NewSyncStorage()
	}))
}

func newRemoveCmd(sdlCreateCb SyncStorageCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove <namespace> <key>",
		Short: "Remove a key under given namespace from SDL DB",
		Long:  `Remove a key under given namespace from SDL DB`,
		Args:  cobra.ExactArgs(2),
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
	if err := sdl.Remove(args[0], []string{args[1]}); err != nil {
		return err
	}
	return nil
}
