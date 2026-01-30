package cli

import (
	"context"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/spf13/cobra"
)

var (
	stashMessage string
	stashRestage bool // --index flag for pop/apply
)

var stashCmd = &cobra.Command{
	Use:   "stash",
	Short: "Stash uncommitted changes",
	Long: `Save uncommitted changes and restore Weaviate to a clean state.

When run without a subcommand, acts as 'stash push'.

Examples:
  wvc stash                       Save all changes to a new stash
  wvc stash -m "work in progress" Save with a custom message
  wvc stash list                  List all stashes
  wvc stash pop                   Apply and remove the latest stash
  wvc stash apply stash@{1}       Apply a specific stash without removing
  wvc stash drop stash@{0}        Remove a specific stash
  wvc stash show                  Show changes in the latest stash
  wvc stash clear                 Remove all stashes`,
	Run: runStashPush,
}

var stashPushCmd = &cobra.Command{
	Use:   "push [-m <message>]",
	Short: "Save changes to a new stash",
	Long:  `Save all uncommitted changes (staged and unstaged) and restore Weaviate to the last committed state.`,
	Run:   runStashPush,
}

var stashListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all stashes",
	Run:   runStashList,
}

var stashPopCmd = &cobra.Command{
	Use:   "pop [stash@{N}]",
	Short: "Apply and remove a stash",
	Long: `Apply the stash and then remove it from the stash list.
By default, all changes come back as unstaged. Use --index to
reinstate previously staged changes to the staging area.`,
	Args: cobra.MaximumNArgs(1),
	Run:  runStashPop,
}

var stashApplyCmd = &cobra.Command{
	Use:   "apply [stash@{N}]",
	Short: "Apply a stash without removing it",
	Long: `Apply the stash changes to Weaviate without removing it from the stash list.
By default, all changes come back as unstaged. Use --index to
reinstate previously staged changes to the staging area.`,
	Args: cobra.MaximumNArgs(1),
	Run:  runStashApply,
}

var stashDropCmd = &cobra.Command{
	Use:   "drop [stash@{N}]",
	Short: "Remove a stash",
	Args:  cobra.MaximumNArgs(1),
	Run:   runStashDrop,
}

var stashShowCmd = &cobra.Command{
	Use:   "show [stash@{N}]",
	Short: "Show changes in a stash",
	Args:  cobra.MaximumNArgs(1),
	Run:   runStashShow,
}

var stashClearCmd = &cobra.Command{
	Use:   "clear",
	Short: "Remove all stashes",
	Run:   runStashClear,
}

func init() {
	stashCmd.Flags().StringVarP(&stashMessage, "message", "m", "", "Stash message")
	stashPushCmd.Flags().StringVarP(&stashMessage, "message", "m", "", "Stash message")
	stashPopCmd.Flags().BoolVar(&stashRestage, "index", false, "Reinstate previously staged changes to the staging area")
	stashApplyCmd.Flags().BoolVar(&stashRestage, "index", false, "Reinstate previously staged changes to the staging area")

	stashCmd.AddCommand(stashPushCmd)
	stashCmd.AddCommand(stashListCmd)
	stashCmd.AddCommand(stashPopCmd)
	stashCmd.AddCommand(stashApplyCmd)
	stashCmd.AddCommand(stashDropCmd)
	stashCmd.AddCommand(stashShowCmd)
	stashCmd.AddCommand(stashClearCmd)
}

func runStashPush(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	opts := core.StashPushOptions{
		Message: stashMessage,
	}

	result, err := core.StashPush(bgCtx, c.Config, c.Store, c.Client, opts)
	if err != nil {
		exitError("%v", err)
	}

	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)

	green.Printf("Saved working directory and index state\n")
	fmt.Printf("  %s\n", result.Message)

	if result.StagedCount > 0 {
		fmt.Printf("  %d staged change(s)\n", result.StagedCount)
	}
	if result.UnstagedCount > 0 {
		fmt.Printf("  %d unstaged change(s)\n", result.UnstagedCount)
	}

	for _, w := range result.Warnings {
		yellow.Printf("Warning: %s\n", w.Message)
	}
}

func runStashList(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	entries, err := core.StashList(c.Store)
	if err != nil {
		exitError("%v", err)
	}

	if len(entries) == 0 {
		fmt.Println("No stashes")
		return
	}

	cyan := color.New(color.FgCyan)
	for _, e := range entries {
		branch := e.BranchName
		if branch == "" {
			branch = "(detached)"
		}
		cyan.Printf("stash@{%d}", e.Index)
		fmt.Printf(": On %s: %s\n", branch, e.Message)
	}
}

func runStashPop(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	index := parseStashArg(args)

	opts := core.StashApplyOptions{
		Index:   index,
		Restage: stashRestage,
	}

	result, err := core.StashPop(bgCtx, c.Config, c.Store, c.Client, opts)
	if err != nil {
		exitError("%v", err)
	}

	displayStashApplyResult(result, true)
}

func runStashApply(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	index := parseStashArg(args)

	opts := core.StashApplyOptions{
		Index:   index,
		Restage: stashRestage,
	}

	result, err := core.StashApply(bgCtx, c.Config, c.Store, c.Client, opts)
	if err != nil {
		exitError("%v", err)
	}

	displayStashApplyResult(result, false)
}

func runStashDrop(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	index := parseStashArg(args)

	msg, err := core.StashDrop(c.Store, index)
	if err != nil {
		exitError("%v", err)
	}

	green := color.New(color.FgGreen)
	green.Printf("Dropped stash@{%d} (%s)\n", index, msg)
}

func runStashShow(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	index := parseStashArg(args)

	result, err := core.StashShow(c.Store, index)
	if err != nil {
		exitError("%v", err)
	}

	cyan := color.New(color.FgCyan)
	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)
	red := color.New(color.FgRed)

	branch := result.BranchName
	if branch == "" {
		branch = "(detached)"
	}
	cyan.Printf("stash@{%d}: On %s: %s\n", index, branch, result.Message)
	fmt.Println()

	if len(result.StagedChanges) > 0 {
		fmt.Printf("Staged changes (%d):\n", len(result.StagedChanges))
		for _, sc := range result.StagedChanges {
			printStashChange(sc, green, yellow, red)
		}
	}

	if len(result.UnstagedChanges) > 0 {
		if len(result.StagedChanges) > 0 {
			fmt.Println()
		}
		fmt.Printf("Unstaged changes (%d):\n", len(result.UnstagedChanges))
		for _, sc := range result.UnstagedChanges {
			printStashChange(sc, green, yellow, red)
		}
	}

	if len(result.StagedChanges) == 0 && len(result.UnstagedChanges) == 0 {
		fmt.Println("No changes in stash")
	}
}

func runStashClear(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	count, err := core.StashClear(c.Store)
	if err != nil {
		exitError("%v", err)
	}

	if count == 0 {
		fmt.Println("No stashes to clear")
	} else {
		green := color.New(color.FgGreen)
		green.Printf("Cleared %d stash(es)\n", count)
	}
}

func parseStashArg(args []string) int {
	if len(args) == 0 {
		return 0
	}
	index, err := core.ParseStashRef(args[0])
	if err != nil {
		exitError("%v", err)
	}
	return index
}

func displayStashApplyResult(result *core.StashApplyResult, dropped bool) {
	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)

	if dropped {
		green.Printf("Applied and dropped stash\n")
	} else {
		green.Printf("Applied stash\n")
	}
	fmt.Printf("  %s\n", result.Message)

	total := result.StagedCount + result.UnstagedCount
	if stashRestage && result.StagedCount > 0 {
		fmt.Printf("  %d change(s) re-staged, %d unstaged change(s) applied\n", result.StagedCount, result.UnstagedCount)
	} else if total > 0 {
		fmt.Printf("  %d change(s) applied\n", total)
	}

	for _, w := range result.Warnings {
		yellow.Printf("Warning: %s\n", w.Message)
	}
}

func printStashChange(sc *models.StashChange, green, yellow, red *color.Color) {
	switch sc.ChangeType {
	case "insert":
		green.Printf("        new:      %s/%s\n", sc.ClassName, shortID(sc.ObjectID))
	case "update":
		yellow.Printf("        modified: %s/%s\n", sc.ClassName, shortID(sc.ObjectID))
	case "delete":
		red.Printf("        deleted:  %s/%s\n", sc.ClassName, shortID(sc.ObjectID))
	}
}
