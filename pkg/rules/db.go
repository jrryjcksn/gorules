package rules

import (
	"database/sql"
	"strings"

	_ "github.com/jrryjcksn/go-sqlite3"
)

var schemaEntries = `
CREATE TABLE instantiations (ID INTEGER PRIMARY KEY, ruleNum INTEGER NOT NULL, priority INTEGER NOT NULL DEFAULT 0, timestamp INTEGER, active BOOL NOT NULL DEFAULT true, resources JSON NOT NULL)
CREATE TRIGGER instantiation_expansion_TRIGGER AFTER INSERT ON instantiations BEGIN UPDATE instantiations SET timestamp = time('now', 'unixepoch') WHERE ID = NEW.ID; END
CREATE TRIGGER instantiation_connection_TRIGGER AFTER INSERT ON instantiations BEGIN INSERT INTO resource_instantiations SELECT value, NEW.ID FROM json_each(NEW.resources); END
CREATE TRIGGER instantiation_delete_TRIGGER AFTER DELETE ON instantiations BEGIN DELETE FROM resource_instantiations WHERE instantiation_ID = OLD.ID; END

CREATE TABLE resource_instantiations (resource_ID INTEGER NOT NULL, instantiation_ID INTEGER NOT NULL)
CREATE INDEX resource_instantiations_INDEX ON resource_instantiations (resource_ID)
CREATE INDEX instantiation_resources_INDEX ON resource_instantiations (instantiation_ID, resource_ID)

CREATE TABLE resources (ID INTEGER PRIMARY KEY, KIND TEXT NOT NULL, NAME TEXT NOT NULL DEFAULT "", NAMESPACE TEXT NOT NULL DEFAULT "", DATA JSON NOT NULL)
CREATE UNIQUE INDEX unique_resource_INDEX ON resources (KIND, NAME, NAMESPACE)
CREATE INDEX namespace_resource_INDEX ON resources (NAMESPACE)
CREATE TRIGGER resource_upsert_TRIGGER BEFORE UPDATE ON resources BEGIN DELETE FROM instantiations WHERE ID IN (SELECT instantiation_ID FROM resource_instantiations WHERE resource_ID = NEW.ID); DELETE FROM resource_instantiations WHERE resource_ID = NEW.ID; END
CREATE TRIGGER resource_delete_TRIGGER AFTER DELETE ON resources BEGIN DELETE FROM instantiations WHERE ID IN (SELECT instantiation_ID FROM resource_instantiations WHERE resource_ID = OLD.ID); DELETE FROM resource_instantiations WHERE resource_ID = OLD.ID; END

CREATE TABLE configuration (name TEXT PRIMARY KEY, val)
`

func getDB() (*sql.DB, error) {
	database, err := sql.Open("sqlite3", "file:storage?mode=memory")
	if err != nil {
		return nil, err
	}

	for _, entry := range strings.Split(schemaEntries, "\n") {
		if entry == "" {
			continue
		}

		statement, err := database.Prepare(entry)
		if err != nil {
			return nil, err
		}

		_, err = statement.Exec()
		if err != nil {
			return nil, err
		}
	}

	return database, nil
}
