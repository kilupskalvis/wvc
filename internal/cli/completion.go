package cli

import (
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(&cobra.Command{
		Use:   "completion [bash|zsh|fish]",
		Short: "Generate shell completion script",
		Long: `Generate shell completion script for wvc.

To load completions:

Bash:
  $ source <(wvc completion bash)
  # Or add to ~/.bashrc:
  $ echo 'source <(wvc completion bash)' >> ~/.bashrc

Zsh:
  $ source <(wvc completion zsh)
  # Or add to ~/.zshrc:
  $ echo 'source <(wvc completion zsh)' >> ~/.zshrc

Fish:
  $ wvc completion fish | source
  # Or add to config:
  $ wvc completion fish > ~/.config/fish/completions/wvc.fish
`,
		ValidArgs:             []string{"bash", "zsh", "fish"},
		Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		DisableFlagsInUseLine: true,
		Run: func(cmd *cobra.Command, args []string) {
			switch args[0] {
			case "bash":
				rootCmd.GenBashCompletion(os.Stdout)
			case "zsh":
				rootCmd.GenZshCompletion(os.Stdout)
			case "fish":
				rootCmd.GenFishCompletion(os.Stdout, true)
			}
		},
	})
}
