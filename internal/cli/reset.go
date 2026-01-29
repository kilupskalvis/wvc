package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var (
	resetTo    string // Hidden, for backwards compatibility
	resetSoft  bool
	resetMixed bool
	resetHard  bool
	resetForce bool
)

var resetCmd = &cobra.Command{
	Use:   "reset [<commit>] [--] [<class>...]",
	Short: "Unstage changes or reset HEAD to a commit",
	Long: `Unstage changes from the staging area, or reset HEAD to a specific commit.

Examples:
  wvc reset                    Unstage all changes
  wvc reset Article            Unstage all Article class changes
  wvc reset HEAD~1             Reset to parent commit (mixed mode)
  wvc reset --soft HEAD~1      Reset to parent, keep changes staged
  wvc reset --hard main        Reset to main, restore Weaviate state
  wvc reset -- main            Force unstage class named "main"

The command tries to resolve the argument as a commit/branch first.
If it doesn't resolve, it's treated as a class/object to unstage.
Use -- to force unstage interpretation.

Reset modes:
  --soft   Move HEAD and branch pointer. Auto-stage changes from undone commits.
           Weaviate unchanged. Use case: Redo last commit with different message.
  --mixed  Move HEAD, clear staging area. Weaviate unchanged. (default)
           Use case: Unstage and recommit differently.
  --hard   Move HEAD, clear staging, restore Weaviate to target state.
           Use case: Discard all changes and go back to a previous state.`,
	Run: runReset,
}

func init() {
	// Hidden flag for backwards compatibility
	resetCmd.Flags().StringVar(&resetTo, "to", "", "Target commit (deprecated, use positional arg)")
	resetCmd.Flags().MarkHidden("to")

	resetCmd.Flags().BoolVar(&resetSoft, "soft", false, "Soft reset: move HEAD and auto-stage changes from undone commits")
	resetCmd.Flags().BoolVar(&resetMixed, "mixed", false, "Mixed reset: move HEAD and clear staging (default)")
	resetCmd.Flags().BoolVar(&resetHard, "hard", false, "Hard reset: move HEAD, clear staging, restore Weaviate state")
	resetCmd.Flags().BoolVarP(&resetForce, "force", "f", false, "Skip confirmation prompt for hard reset")
}

func runReset(cmd *cobra.Command, args []string) {
	hasModeFlag := resetSoft || resetMixed || resetHard

	// Backwards compat: if --to is used, use old behavior
	if resetTo != "" {
		runResetToCommit(resetTo)
		return
	}

	// If mode flags present, first arg must be commit
	if hasModeFlag {
		if len(args) == 0 {
			exitError("commit reference required with --soft/--mixed/--hard")
		}
		runResetToCommit(args[0])
		return
	}

	// No mode flags - need to disambiguate
	if len(args) == 0 {
		// wvc reset - unstage all
		runUnstage(args)
		return
	}

	// Check if -- was used (forces unstage interpretation)
	// ArgsLenAtDash returns -1 if -- was not used, otherwise the index
	dashIdx := cmd.ArgsLenAtDash()
	if dashIdx == 0 {
		// wvc reset -- <args> : everything is after --, force unstage
		runUnstage(args)
		return
	}

	// Try to resolve first arg as commit/branch
	c := initContextWithMigrations()
	_, _, err := core.ResolveRef(c.Store, args[0])
	c.Close()

	if err == nil {
		// Resolves as commit/branch - do mixed reset
		runResetToCommit(args[0])
	} else {
		// Doesn't resolve - treat as class/object for unstage
		runUnstage(args)
	}
}

func runResetToCommit(target string) {
	// Validate mutually exclusive flags
	modeCount := 0
	if resetSoft {
		modeCount++
	}
	if resetMixed {
		modeCount++
	}
	if resetHard {
		modeCount++
	}
	if modeCount > 1 {
		exitError("cannot use --soft, --mixed, and --hard together")
	}

	// Determine mode (default to mixed)
	mode := core.ResetModeMixed
	if resetSoft {
		mode = core.ResetModeSoft
	} else if resetHard {
		mode = core.ResetModeHard
	}

	// Confirm hard reset unless --force
	if mode == core.ResetModeHard && !resetForce {
		fmt.Print("Hard reset will discard all uncommitted changes and restore Weaviate state. Continue? [y/N] ")
		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))
		if response != "y" && response != "yes" {
			fmt.Println("Aborted.")
			return
		}
	}

	// Initialize full context (need Weaviate client for hard reset)
	ctx := context.Background()
	c := initFullContext()
	defer c.Close()

	opts := core.ResetOptions{
		Mode: mode,
	}

	result, err := core.ResetToCommit(ctx, c.Config, c.Store, c.Client, target, opts)
	if err != nil {
		exitError("%v", err)
	}

	// Display results
	displayResetResult(result)
}

func displayResetResult(result *core.ResetResult) {
	green := color.New(color.FgGreen)
	yellow := color.New(color.FgYellow)
	cyan := color.New(color.FgCyan)

	// Get commit info for display
	shortID := result.TargetCommit
	if len(shortID) > 7 {
		shortID = shortID[:7]
	}

	green.Printf("Reset to %s (%s)\n", shortID, result.Mode.String())

	if result.BranchName != "" {
		cyan.Printf("Branch '%s' now at %s\n", result.BranchName, shortID)
	} else {
		cyan.Printf("HEAD now at %s (detached)\n", shortID)
	}

	// Report staging changes
	if result.Mode == core.ResetModeSoft && result.ChangesStaged > 0 {
		yellow.Printf("Staged %d change(s)\n", result.ChangesStaged)
	} else if result.Mode != core.ResetModeSoft && result.StagedCleared > 0 {
		yellow.Printf("Cleared %d staged change(s)\n", result.StagedCleared)
	}

	// Report Weaviate changes for hard reset
	if result.Mode == core.ResetModeHard {
		if result.ObjectsAdded > 0 || result.ObjectsUpdated > 0 || result.ObjectsRemoved > 0 {
			fmt.Printf("Restored: %d added, %d updated, %d removed\n",
				result.ObjectsAdded, result.ObjectsUpdated, result.ObjectsRemoved)
		}
	}

	// Show warnings
	for _, w := range result.Warnings {
		yellow.Printf("Warning: %s\n", w.Message)
	}
}

func runUnstage(args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	st := c.Store
	yellow := color.New(color.FgYellow)

	if len(args) == 0 {
		// Unstage all
		count, err := st.GetStagedChangesCount()
		if err != nil {
			exitError("failed to get staged count: %v", err)
		}

		if count == 0 {
			fmt.Println("Nothing to unstage")
			return
		}

		if err := core.UnstageAll(st); err != nil {
			exitError("failed to unstage: %v", err)
		}
		yellow.Printf("Unstaged %d change(s)\n", count)
		return
	}

	for _, arg := range args {
		className, objectID, err := core.ParseObjectRef(arg)
		if err != nil {
			exitError("%v", err)
		}

		if objectID == "" {
			// Unstage entire class
			changes, err := st.GetStagedChangesByClass(className)
			if err != nil {
				exitError("failed to get staged changes: %v", err)
			}

			if len(changes) == 0 {
				fmt.Printf("No staged changes for %s\n", className)
				continue
			}

			if err := core.UnstageClass(st, className); err != nil {
				exitError("failed to unstage %s: %v", className, err)
			}
			yellow.Printf("Unstaged %d change(s) from %s\n", len(changes), className)
		} else {
			// Unstage specific object
			staged, err := st.GetStagedChange(className, objectID)
			if err != nil {
				exitError("failed to check staged change: %v", err)
			}
			if staged == nil {
				fmt.Printf("No staged changes for %s/%s\n", className, objectID)
				continue
			}

			if err := core.UnstageObject(st, className, objectID); err != nil {
				exitError("failed to unstage %s/%s: %v", className, objectID, err)
			}
			yellow.Printf("Unstaged %s/%s\n", className, objectID)
		}
	}
}
