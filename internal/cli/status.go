package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show the working tree status",
	Long:  `Show the current status of the Weaviate database compared to the last commit.`,
	Run:   runStatus,
}

func runStatus(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	st, client := c.Store, c.Client

	// Show branch info
	currentBranch, _ := st.GetCurrentBranch()
	head, _ := st.GetHEAD()

	if currentBranch != "" {
		fmt.Printf("On branch %s\n", currentBranch)
	} else if head != "" {
		fmt.Printf("HEAD detached at %s\n", shortID(head))
	}

	if head != "" {
		commit, err := st.GetCommit(head)
		if err == nil && currentBranch != "" {
			fmt.Printf("Commit: %s\n", commit.ShortID())
		}
	} else {
		fmt.Println("No commits yet")
	}

	schemaDiff, err := core.ComputeSchemaDiff(bgCtx, st, client)
	if err != nil {
		schemaDiff = &core.SchemaDiffResult{}
	}

	diff, err := core.ComputeIncrementalDiff(bgCtx, c.Config, st, client)
	if err != nil {
		exitError("failed to compute diff: %v", err)
	}

	stagedCount := diff.TotalStagedChanges()
	unstagedCount := diff.TotalUnstagedChanges()
	schemaChanges := schemaDiff.TotalChanges()

	if stagedCount == 0 && unstagedCount == 0 && schemaChanges == 0 {
		fmt.Println("\nNothing to commit, working tree clean")
		return
	}

	green := color.New(color.FgGreen)
	red := color.New(color.FgRed)
	yellow := color.New(color.FgYellow)
	cyan := color.New(color.FgCyan)
	magenta := color.New(color.FgMagenta)

	// Show schema changes first
	if schemaChanges > 0 {
		fmt.Println("\nSchema changes:")
		cyan.Println("  (schema changes are committed automatically with data)")
		fmt.Println()
		printSchemaChanges(schemaDiff, green, yellow, red, magenta, "        ")
	}

	// Show staged changes
	if stagedCount > 0 {
		fmt.Println("\nChanges to be committed:")
		cyan.Println("  (use \"wvc reset <class>/<id>\" to unstage)")
		fmt.Println()

		printChanges(diff.Staged, green, yellow, red, "        ")
	}

	// Show unstaged changes
	if unstagedCount > 0 {
		fmt.Println("\nChanges not staged for commit:")
		cyan.Println("  (use \"wvc add <class>/<id>\" to stage)")
		fmt.Println()

		printChanges(diff.Unstaged, green, yellow, red, "        ")
	}

	// Summary
	fmt.Println()
	parts := []string{}
	if schemaChanges > 0 {
		parts = append(parts, fmt.Sprintf("%d schema", schemaChanges))
	}
	if stagedCount > 0 {
		parts = append(parts, fmt.Sprintf("%d staged", stagedCount))
	}
	if unstagedCount > 0 {
		parts = append(parts, fmt.Sprintf("%d unstaged", unstagedCount))
	}

	if len(parts) > 0 {
		fmt.Println(strings.Join(parts, ", "))
	}

	if stagedCount > 0 || schemaChanges > 0 {
		fmt.Println("\nUse 'wvc commit -m \"message\"' to commit changes.")
	} else if unstagedCount > 0 {
		fmt.Println("\nUse 'wvc add .' to stage all changes.")
	}
}

// printChanges prints a diff result with color coding
func printChanges(diff *core.DiffResult, green, yellow, red *color.Color, indent string) {
	if len(diff.Inserted) > 0 {
		for _, change := range diff.Inserted {
			green.Printf("%snew:      %s/%s\n", indent, change.ClassName, shortID(change.ObjectID))
		}
	}

	if len(diff.Updated) > 0 {
		for _, change := range diff.Updated {
			if change.VectorOnly {
				yellow.Printf("%smodified (vector): %s/%s\n", indent, change.ClassName, shortID(change.ObjectID))
			} else {
				yellow.Printf("%smodified: %s/%s\n", indent, change.ClassName, shortID(change.ObjectID))
			}
		}
	}

	if len(diff.Deleted) > 0 {
		for _, change := range diff.Deleted {
			red.Printf("%sdeleted:  %s/%s\n", indent, change.ClassName, shortID(change.ObjectID))
		}
	}
}

// printSchemaChanges prints schema changes with color coding
func printSchemaChanges(diff *core.SchemaDiffResult, green, yellow, red, magenta *color.Color, indent string) {
	// Classes added
	for _, change := range diff.ClassesAdded {
		green.Printf("%snew class:      %s\n", indent, change.ClassName)
	}

	// Classes deleted
	for _, change := range diff.ClassesDeleted {
		red.Printf("%sdeleted class:  %s\n", indent, change.ClassName)
	}

	// Properties added
	for _, change := range diff.PropertiesAdded {
		green.Printf("%snew property:   %s.%s\n", indent, change.ClassName, change.PropertyName)
	}

	// Properties deleted
	for _, change := range diff.PropertiesDeleted {
		red.Printf("%sdeleted prop:   %s.%s\n", indent, change.ClassName, change.PropertyName)
	}

	// Properties modified
	for _, change := range diff.PropertiesModified {
		yellow.Printf("%smodified prop:  %s.%s\n", indent, change.ClassName, change.PropertyName)
	}

	// Vectorizer changes
	for _, change := range diff.VectorizersChanged {
		magenta.Printf("%svectorizer:     %s\n", indent, change.ClassName)
	}
}
