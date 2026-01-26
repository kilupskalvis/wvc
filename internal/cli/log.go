package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Show commit history",
	Long:  `Display the commit history of the repository.`,
	Run:   runLog,
}

var (
	logOneline bool
	logLimit   int
)

func init() {
	logCmd.Flags().BoolVar(&logOneline, "oneline", false, "Show each commit on a single line")
	logCmd.Flags().IntVarP(&logLimit, "n", "n", 0, "Limit the number of commits to show")
}

func runLog(cmd *cobra.Command, args []string) {
	c := initContext()
	defer c.Close()

	st := c.Store
	commits, err := st.GetCommitLog(logLimit)
	if err != nil {
		exitError("failed to get commit log: %v", err)
	}

	if len(commits) == 0 {
		fmt.Println("No commits yet")
		return
	}

	head, _ := st.GetHEAD()
	yellow := color.New(color.FgYellow)

	magenta := color.New(color.FgMagenta)

	for _, commit := range commits {
		isHead := commit.ID == head

		// Check if commit has schema changes
		hasSchemaChange, _ := st.CommitHasSchemaChange(commit.ID)

		if logOneline {
			if isHead {
				yellow.Printf("%s ", commit.ShortID())
				color.New(color.FgCyan).Print("(HEAD) ")
			} else {
				yellow.Printf("%s ", commit.ShortID())
			}
			if hasSchemaChange {
				magenta.Print("[schema] ")
			}
			fmt.Println(commit.Message)
		} else {
			if isHead {
				yellow.Printf("commit %s ", commit.ID)
				color.New(color.FgCyan).Print("(HEAD)")
				if hasSchemaChange {
					magenta.Print(" [schema]")
				}
				fmt.Println()
			} else {
				yellow.Printf("commit %s", commit.ID)
				if hasSchemaChange {
					magenta.Print(" [schema]")
				}
				fmt.Println()
			}
			fmt.Printf("Date:   %s\n", commit.Timestamp.Format("Mon Jan 2 15:04:05 2006"))
			fmt.Printf("\n    %s\n", commit.Message)
			fmt.Printf("    (%d operations)\n\n", commit.OperationCount)
		}
	}
}
