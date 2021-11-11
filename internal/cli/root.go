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

func NewRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   SdlCliApp,
		Short: "Shared Data Layer (SDL) troubleshooting command line tool",
		Long:  `Shared Data Layer (SDL) troubleshooting command line tool`,
		Run: func(cmd *cobra.Command, args []string) {
		},
	}
	cmd.AddCommand(NewHealthCheckCmd())
	cmd.AddCommand(NewSetCmd())
	return cmd
}
