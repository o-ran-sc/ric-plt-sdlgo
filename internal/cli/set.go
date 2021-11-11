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
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo"
	"gerrit.o-ran-sc.org/r/ric-plt/sdlgo/internal/sdlgoredis"
	"github.com/spf13/cobra"
	"os"
)

func NewSetCmd() *cobra.Command {
	return newSetCmd(func() ISyncStorage {
		return sdlgo.NewSyncStorage()
	})
}

func newSetCmd(sdlCreateCb SyncStorageCreateCb) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set <namespace> <key> <value>",
		Short: "set - set key-value pair to SDL DB",
		Long:  `set - set key-value pair to SDL DB`,
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			var buf bytes.Buffer
			sdlgoredis.SetDbLogger(&buf)
			err := runSet(sdlCreateCb, args)
			if err != nil {
				cmd.Println(err.Error())
				cmd.PrintErrf("%s\n", buf.String())
			}
			return err
		},
	}
	cmd.SetOut(os.Stdout)
	return cmd
}

func runSet(sdlCreateCb SyncStorageCreateCb, args []string) error {
	sdl := sdlCreateCb()
	if len(args) > 3 {
		return fmt.Errorf("requires only 3 arg(s), received %d", len(args))
	}
	if err := sdl.Set(args[0], args[1], args[2]); err != nil {
		return fmt.Errorf("SDL CLI error: %s", err.Error())
	}
	return nil
}
