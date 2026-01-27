package store

const currentSchemaVersion = 1

// RunMigrations applies any pending database migrations.
// All features are included in the base schema (sqlite.go).
// Migrations will be added here when schema changes are needed
func (s *Store) RunMigrations() error {
	return nil
}
