package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "Show changes between commits and working tree",
	Long:  `Show the differences between the current Weaviate state and the last commit.`,
	Run:   runDiff,
}

var (
	diffStat   bool
	diffSchema bool
)

func init() {
	diffCmd.Flags().BoolVar(&diffStat, "stat", false, "Show diffstat instead of full diff")
	diffCmd.Flags().BoolVar(&diffSchema, "schema", false, "Show schema changes only")
}

func runDiff(cmd *cobra.Command, args []string) {
	bgCtx := context.Background()
	c := initFullContext()
	defer c.Close()

	cfg, st, client := c.Config, c.Store, c.Client
	green := color.New(color.FgGreen)
	red := color.New(color.FgRed)
	yellow := color.New(color.FgYellow)
	magenta := color.New(color.FgMagenta)

	if diffSchema {
		schemaDiff, err := core.ComputeSchemaDiff(bgCtx, st, client)
		if err != nil {
			exitError("failed to compute schema diff: %v", err)
		}

		if !schemaDiff.HasChanges() {
			fmt.Println("No schema changes")
			return
		}

		displaySchemaDiff(schemaDiff, green, red, yellow, magenta)
		return
	}

	diff, err := core.ComputeDiff(bgCtx, cfg, st, client)
	if err != nil {
		exitError("failed to compute diff: %v", err)
	}

	if diff.TotalChanges() == 0 {
		fmt.Println("No changes")
		return
	}

	if diffStat {
		// Show summary only
		if len(diff.Inserted) > 0 {
			green.Printf(" %d insertions(+)\n", len(diff.Inserted))
		}
		if len(diff.Updated) > 0 {
			yellow.Printf(" %d modifications(~)\n", len(diff.Updated))
		}
		if len(diff.Deleted) > 0 {
			red.Printf(" %d deletions(-)\n", len(diff.Deleted))
		}
		fmt.Printf(" %d objects changed\n", diff.TotalChanges())
		return
	}

	// Show full diff
	for _, change := range diff.Inserted {
		green.Printf("+++ %s/%s\n", change.ClassName, change.ObjectID)
		if change.CurrentData != nil {
			data, _ := json.MarshalIndent(change.CurrentData.Properties, "    ", "  ")
			green.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}

	for _, change := range diff.Deleted {
		red.Printf("--- %s/%s\n", change.ClassName, change.ObjectID)
		if change.PreviousData != nil {
			data, _ := json.MarshalIndent(change.PreviousData.Properties, "    ", "  ")
			red.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}

	for _, change := range diff.Updated {
		yellow.Printf("~~~ %s/%s\n", change.ClassName, change.ObjectID)
		if change.PreviousData != nil && change.CurrentData != nil {
			fmt.Println("  Before:")
			prevData, _ := json.MarshalIndent(change.PreviousData.Properties, "    ", "  ")
			red.Printf("    %s\n", string(prevData))
			fmt.Println("  After:")
			currData, _ := json.MarshalIndent(change.CurrentData.Properties, "    ", "  ")
			green.Printf("    %s\n", string(currData))
		}
		fmt.Println()
	}
}

// displaySchemaDiff shows schema changes with +++ / --- / ~~~ formatting
func displaySchemaDiff(diff *core.SchemaDiffResult, green, red, yellow, magenta *color.Color) {
	// Added classes
	for _, change := range diff.ClassesAdded {
		green.Printf("+++ class %s\n", change.ClassName)
		if change.CurrentValue != nil {
			data, _ := json.MarshalIndent(change.CurrentValue, "    ", "  ")
			green.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}

	// Deleted classes
	for _, change := range diff.ClassesDeleted {
		red.Printf("--- class %s\n", change.ClassName)
		if change.PreviousValue != nil {
			data, _ := json.MarshalIndent(change.PreviousValue, "    ", "  ")
			red.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}

	// Added properties
	for _, change := range diff.PropertiesAdded {
		green.Printf("+++ property %s.%s\n", change.ClassName, change.PropertyName)
		if change.CurrentValue != nil {
			data, _ := json.MarshalIndent(change.CurrentValue, "    ", "  ")
			green.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}

	// Deleted properties
	for _, change := range diff.PropertiesDeleted {
		red.Printf("--- property %s.%s\n", change.ClassName, change.PropertyName)
		if change.PreviousValue != nil {
			data, _ := json.MarshalIndent(change.PreviousValue, "    ", "  ")
			red.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}

	// Modified properties
	for _, change := range diff.PropertiesModified {
		yellow.Printf("~~~ property %s.%s\n", change.ClassName, change.PropertyName)
		if change.PreviousValue != nil {
			fmt.Println("  Before:")
			data, _ := json.MarshalIndent(change.PreviousValue, "    ", "  ")
			red.Printf("    %s\n", string(data))
		}
		if change.CurrentValue != nil {
			fmt.Println("  After:")
			data, _ := json.MarshalIndent(change.CurrentValue, "    ", "  ")
			green.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}

	// Vectorizer changes
	for _, change := range diff.VectorizersChanged {
		magenta.Printf("~~~ vectorizer %s\n", change.ClassName)
		if change.PreviousValue != nil {
			fmt.Println("  Before:")
			data, _ := json.MarshalIndent(change.PreviousValue, "    ", "  ")
			red.Printf("    %s\n", string(data))
		}
		if change.CurrentValue != nil {
			fmt.Println("  After:")
			data, _ := json.MarshalIndent(change.CurrentValue, "    ", "  ")
			green.Printf("    %s\n", string(data))
		}
		fmt.Println()
	}
}
