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
	"github.com/spf13/cobra"
)

var getCmd = newGetCmd()

func init() {
	rootCmd.AddCommand(getCmd)
}

var (
	getLong = `Display one or many resources.

Prints important information about the specified resources.`

	getExample = `  # List keys in the given namespace.
  sdlcli get keys sdlns`
)

func newGetCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "get",
		Short:   "Display one or many resources",
		Long:    getLong,
		Example: getExample,
	}
}
