package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/kilupskalvis/wvc/internal/core"
	"github.com/kilupskalvis/wvc/internal/models"
	"github.com/kilupskalvis/wvc/internal/store"
	"github.com/spf13/cobra"
)

var showCmd = &cobra.Command{
	Use:   "show [commit]",
	Short: "Show commit details",
	Long:  `Show details about a specific commit including all operations.`,
	Args:  cobra.MaximumNArgs(1),
	Run:   runShow,
}

func runShow(cmd *cobra.Command, args []string) {
	c := initContextWithMigrations()
	defer c.Close()

	st := c.Store
	var commitID string
	var err error
	if len(args) > 0 {
		commitID = args[0]
	} else {
		// Default to HEAD
		commitID, err = st.GetHEAD()
		if err != nil || commitID == "" {
			exitError("no commits yet")
		}
	}

	// Get the commit
	commit, err := st.GetCommit(commitID)
	if err != nil {
		// Try short ID
		commit, err = st.GetCommitByShortID(commitID)
		if err != nil {
			exitError("commit not found: %s", commitID)
		}
	}

	// Display commit info
	yellow := color.New(color.FgYellow)
	green := color.New(color.FgGreen)
	red := color.New(color.FgRed)
	magenta := color.New(color.FgMagenta)

	// Check for schema changes
	hasSchemaChange, _ := st.CommitHasSchemaChange(commit.ID)

	yellow.Printf("commit %s", commit.ID)
	if hasSchemaChange {
		magenta.Print(" [schema]")
	}
	fmt.Println()

	if commit.ParentID != "" {
		fmt.Printf("Parent: %s\n", shortID(commit.ParentID))
	}
	fmt.Printf("Date:   %s\n", commit.Timestamp.Format("Mon Jan 2 15:04:05 2006"))
	fmt.Printf("\n    %s\n\n", commit.Message)

	// Show schema changes if present
	if hasSchemaChange {
		showCommitSchemaChanges(st, commit.ID, green, red, yellow, magenta)
	}

	// Get operations for this commit
	operations, err := st.GetOperationsByCommit(commit.ID)
	if err != nil {
		exitError("failed to get operations: %v", err)
	}

	if len(operations) == 0 && !hasSchemaChange {
		fmt.Println("No operations in this commit")
		return
	}

	if len(operations) > 0 {
		fmt.Printf("Data Operations (%d):\n", len(operations))
		for _, op := range operations {
			switch op.Type {
			case models.OperationInsert:
				green.Printf("  + INSERT %s/%s\n", op.ClassName, shortID(op.ObjectID))
			case models.OperationUpdate:
				yellow.Printf("  ~ UPDATE %s/%s\n", op.ClassName, shortID(op.ObjectID))
			case models.OperationDelete:
				red.Printf("  - DELETE %s/%s\n", op.ClassName, shortID(op.ObjectID))
			}
		}
	}
}

// showCommitSchemaChanges displays schema changes for a commit
func showCommitSchemaChanges(st *store.Store, commitID string, green, red, yellow, magenta *color.Color) {
	currentSchema, err := st.GetSchemaVersionByCommit(commitID)
	if err != nil || currentSchema == nil {
		return
	}

	parentSchema, _ := st.GetPreviousCommitSchema(commitID)

	var prevJSON []byte
	if parentSchema != nil {
		prevJSON = parentSchema.SchemaJSON
	}

	schemaDiff, err := core.ComputeSchemaDiffBetweenVersions(currentSchema.SchemaJSON, prevJSON)
	if err != nil || !schemaDiff.HasChanges() {
		return
	}

	fmt.Println("Schema Changes:")

	for _, change := range schemaDiff.ClassesAdded {
		green.Printf("  + CLASS %s\n", change.ClassName)
	}
	for _, change := range schemaDiff.ClassesDeleted {
		red.Printf("  - CLASS %s\n", change.ClassName)
	}
	for _, change := range schemaDiff.PropertiesAdded {
		green.Printf("  + PROPERTY %s.%s\n", change.ClassName, change.PropertyName)
	}
	for _, change := range schemaDiff.PropertiesDeleted {
		red.Printf("  - PROPERTY %s.%s\n", change.ClassName, change.PropertyName)
	}
	for _, change := range schemaDiff.PropertiesModified {
		yellow.Printf("  ~ PROPERTY %s.%s\n", change.ClassName, change.PropertyName)
	}
	for _, change := range schemaDiff.VectorizersChanged {
		magenta.Printf("  ~ VECTORIZER %s\n", change.ClassName)
	}

	fmt.Println()
}
