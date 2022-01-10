package cli

import (
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(completionCmd())
}

var completionLong = `To load completions:
Bash:

$ source <(sdlcli completion bash)

# To load completions for each session, execute once:
$ sdlcli completion bash > /etc/bash_completion.d/sdlcli
`

func completionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "completion [bash]",
		Short:                 "Generate shell completion script",
		Long:                  completionLong,
		DisableFlagsInUseLine: true,
		ValidArgs:             []string{"bash"},
		Args:                  cobra.ExactValidArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			switch args[0] {
			case "bash":
				cmd.Root().GenBashCompletion(os.Stdout)
			}
		},
	}
	return cmd
}
