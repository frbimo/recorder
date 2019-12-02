package recorder

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
)

var (
	databaseName string

	planName = "Shared"

	statusDeleted = "delete"
	statusExist   = "exist"
)

var (
	stages = map[string]string{}
	_      = syntaxPq()
)

func syntaxPq() map[string]string {

	stages["database"] = fmt.Sprint("CREATE DATABASE %s;", databaseName)

	stages["credentials"] = fmt.Sprint(`CREATE TABLE IF NOT EXISTS credentials (
		database_pid 			SERIAL  PRIMARY KEY	NOT NULL,
		pseudo_id	 			VARCHAR(50) 			NOT NULL,
		shared_use 				BOOLEAN 			NOT NULL,
		uri 					VARCHAR(500),		
		username 				VARCHAR(50),	
		password 				VARCHAR(50),
		database				VARCHAR(50)
		)`)

	stages["ip"] = fmt.Sprint(`CREATE TABLE IF NOT EXISTS ip (
			ip_id 					SERIAL  PRIMARY KEY				NOT NULL,
			database_pid	 		SERIAL REFERENCES credentials(database_pid) 	NOT NULL,
			internal_ip 			INET	 						NOT NULL,
			public_ip 				INET,		
			port 					INTEGER,	
			in_use 					BOOLEAN	
			)`)

	stages["usage"] = fmt.Sprint(`CREATE TABLE IF NOT EXISTS usage (
				usage_pid	 			SERIAL  PRIMARY KEY				NOT NULL,
				database_pid			SERIAL REFERENCES credentials(database_pid)	NOT NULL,
				cpu 					VARCHAR(16)						NOT NULL,
				memory 					VARCHAR(16)						NOT NULL,
				usage_calculate			VARCHAR(16)						NOT NULL
				)`)

	stages["shareInstance"] = fmt.Sprint(`CREATE TABLE IF NOT EXISTS share_instance (
					share_instance_pid 		SERIAL  PRIMARY KEY 			NOT NULL,
					database_pid			SERIAL REFERENCES credentials(database_pid)	NOT NULL,
					instances_id 			VARCHAR(50) 						NOT NULL,
					plan_id 				VARCHAR(50) 						NOT NULL,
					plan_name 				VARCHAR(50) 						NOT NULL,
					org_id 					VARCHAR(50) 						NOT NULL,
					spaces_id 				VARCHAR(50)						NOT NULL,
					create_instance_time 	timestamptz,
					delete_instance_time 	timestamptz,
					instance_status			VARCHAR(16),
					instance_parameters		VARCHAR(256) 
				)`)
	stages["shareBinding"] = fmt.Sprint(`CREATE TABLE IF NOT EXISTS share_binding (
					share_binding_pid 		SERIAL  PRIMARY KEY					NOT NULL,
					share_instance_pid 		SERIAL REFERENCES share_instance(share_instance_pid)	NOT NULL,
					binding_id 				VARCHAR(50) 							NOT NULL,
					binding_time			timestamptz 	 							NOT NULL,
					binding_status			VARCHAR(16),
					unbinding_time 			timestamptz, 		
					binding_parameters		VARCHAR(256) 			
				)`)

	return stages
}

func createDatabase(db *sql.DB) {
	if _, err := db.Exec(stages["database"]); err != nil {
		panic(err)
	}
}

func initStages(db *sql.DB) error {
	sortStages := []string{"credentials", "ip", "usage", "shareInstance", "shareBinding"}
	for _, v := range sortStages {
		if _, err := db.Exec(stages[v]); err != nil {
			return err
		}
	}

	if err := initInsertion(db); err != nil {
		return err
	}
	return nil
}

func initInsertion(db *sql.DB) error {
	var cred, uri string

	databaseName := os.Getenv("DB_KIND")
	databaseName = strings.ToLower(databaseName)

	if strings.Contains(databaseName, "mongo") {
		databaseName = "mongodb"
	} else if strings.Contains(databaseName, "postgre") {
		databaseName = "postgres"
	} else {
		return fmt.Errorf("cannot detect kind of DB")
	}

	pseudoID := os.Getenv("PSEUDO_ID")
	shared := os.Getenv("SHARED_USE")
	dbAuthz := os.Getenv("DATABASE")

	host1 := os.Getenv(fmt.Sprintf("%s_HOST1", strings.ToUpper(databaseName)))
	host2 := os.Getenv(fmt.Sprintf("%s_HOST2", strings.ToUpper(databaseName)))
	host3 := os.Getenv(fmt.Sprintf("%s_HOST3", strings.ToUpper(databaseName)))

	port1 := os.Getenv(fmt.Sprintf("%s_PORT1", strings.ToUpper(databaseName)))
	port2 := os.Getenv(fmt.Sprintf("%s_PORT2", strings.ToUpper(databaseName)))
	port3 := os.Getenv(fmt.Sprintf("%s_PORT3", strings.ToUpper(databaseName)))

	username := os.Getenv(fmt.Sprintf("%s_USERNAME", strings.ToUpper(databaseName)))
	password := os.Getenv(fmt.Sprintf("%s_PASSWORD", strings.ToUpper(databaseName)))

	if username != "" && password != "" {
		cred = fmt.Sprintf("%s:%s@", username, password)
	}

	if !stringValidator(host1, host2, host3) && !stringValidator(port1, port2, port3) {
		uri = fmt.Sprintf("%s://%s%s:%s/%s", databaseName, cred, host1, port1, dbAuthz)
	} else {
		uri = fmt.Sprintf("%s://%s%s:%s,%s:%s,%s:%s/%s", databaseName, cred, host1, port1, host2, port2, host3, port3, dbAuthz)
	}

	if _, err := db.Exec(fmt.Sprint(`INSERT INTO credentials (pseudo_id, shared_use, uri, username, password,
		database) VALUES ($1, $2, $3, $4, $5, $6)`), pseudoID, shared, uri, username, password, dbAuthz); err != nil {
		return err
	}
	return nil
}

func stringValidator(str ...string) bool {
	for i := 0; i < len(str)-1; i++ {
		if strings.Compare(str[i], str[i+1]) != 0 {
			return true
		}
	}
	return false
}
