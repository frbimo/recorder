package recorder

import (
	"database/sql"
	"fmt"
	"os"
)

var (
	databaseName = "mongodb"
	planName     = "Shared"

	statusDeleted = "delete"
	statusExist   = "exist"
)

var (
	stages = map[string]string{}
	_      = syntaxPq()
)

func syntaxPq() map[string]string {

	stages["database"] = fmt.Sprint("CREATE DATABASE mongodb;")

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
	var cred string

	pseudoID := os.Getenv("PSEUDO_ID")
	shared := os.Getenv("SHARED_USE")
	dbAuthz := os.Getenv("DATABASE")

	host1 := os.Getenv("MONGODB_HOST1")
	host2 := os.Getenv("MONGODB_HOST2")
	host3 := os.Getenv("MONGODB_HOST3")

	port1 := os.Getenv("MONGODB_PORT1")
	port2 := os.Getenv("MONGODB_PORT2")
	port3 := os.Getenv("MONGODB_PORT3")

	username := os.Getenv("MONGODB_USERNAME")
	password := os.Getenv("MONGODB_PASSWORD")

	if username != "" && password != "" {
		cred = fmt.Sprintf("%s:%s@", username, password)
	}

	uri := fmt.Sprintf("mongodb://%s%s:%s,%s:%s,%s:%s/%s", cred, host1, port1, host2, port2, host3, port3, dbAuthz)

	if _, err := db.Exec(fmt.Sprint(`INSERT INTO credentials (pseudo_id, shared_use, uri, username, password,
		database) VALUES ($1, $2, $3, $4, $5, $6)`), pseudoID, shared, uri, username, password, dbAuthz); err != nil {
		return err
	}
	return nil
}
