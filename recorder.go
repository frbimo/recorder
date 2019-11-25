package recorder

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	osb "github.com/pmorie/go-open-service-broker-client/v2"
	"github.com/pmorie/osb-broker-lib/pkg/broker"

	// use postgresql
	_ "github.com/lib/pq"
)

const (
	// InstanceIDInUse declared if requested instance ID is exist in ops database.
	InstanceIDInUse = "InstanceID in use"

	// InstanceIDNotFound declared if requested instance ID is exist in ops database.
	InstanceIDNotFound = "InstanceID not found"
)

// CheckConnection do check connection on ops database before all process begin
func CheckConnection() error {
	glog.V(2).Info("Checking database connection ...")
	uri := createURI(databaseName)
	db, err := sql.Open("postgres", uri)
	if err != nil {
		glog.Fatal(err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {

		if strings.Contains(err.Error(), "does not exist") {
			initDatabase()
		} else if strings.Contains(err.Error(), "connect: connection refused") {
			retryConnection(db)
		}
	}
	return nil
}

func retryConnection(db *sql.DB) {
	timer := time.NewTimer(time.Second * 30)
	done := make(chan bool, 1)

	go func() {
		for {
			select {
			case <-timer.C:
				glog.Fatal("failed to establish connection")
			case <-done:
				return
			}
		}
	}()

	var i int
	err := db.Ping()
	for err != nil {
		i++

		time.Sleep(time.Second * 5)
		glog.Errorf("%s, reconnecting ... [%d]", err.Error(), i)
		err = db.Ping()
	}
	done <- true
}

func initDatabase() {
	uri := createURI("postgres")
	db, _ := sql.Open("postgres", uri)
	defer db.Close()

	glog.Warning("creating database ...")
	createDatabase(db)
}

// InitRecorder does init on recorder
func InitRecorder() error {
	uri := createURI(databaseName)
	db, err := sql.Open("postgres", uri)
	if err == nil {
		var check string
		if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM credentials)`).Scan(&check); err != nil {
			glog.Error(err)
			initStages(db)
		}
	}
	return nil
}

// Recorder struct
type Recorder struct {
	Cli *sql.DB

	Ch chan<- cred
}

type cred struct {
	pid     string
	shared  bool
	uri     string
	user    string
	pass    string
	dbAuthz string
}

// New initiate connection
func New() *Recorder {

	uri := createURI(databaseName)
	db, err := sql.Open("postgres", uri)
	if err != nil {
		glog.Error(err)
	}
	return &Recorder{Cli: db}
}

// OnProvision validates and record all Provision ops log then store into ops db
func (r *Recorder) OnProvision(fn ObjectProvision) ObjectProvision {
	return func(request *osb.ProvisionRequest, c *broker.RequestContext) (*broker.ProvisionResponse, error) {

		var id, status string
		errc := make(chan error)

		// Check instance_id and instance_status to see if instance already exists
		if e := r.Cli.QueryRow(`SELECT instances_id, instance_status FROM share_instance where instances_id = $1`,
			request.InstanceID).Scan(&id, &status); e != nil {
			if e != sql.ErrNoRows {
				errString := errToString(e)
				return nil, osb.HTTPStatusCodeError{
					StatusCode:   http.StatusInternalServerError,
					ErrorMessage: &errString,
				}
			}
		}

		if id == request.InstanceID && status == statusExist {
			errString := errToString(fmt.Errorf(InstanceIDInUse))
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusConflict,
				ErrorMessage: &errString,
			}
		}

		var uri, pid *string

		// Get credential and ip from ops DB
		// Query database target information for the service
		if e := r.Cli.QueryRow(`SELECT database_pid, uri FROM credentials where shared_use = true LIMIT 1`).Scan(&pid, &uri); e != nil {
			errString := errToString(e)
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusConflict,
				ErrorMessage: &errString,
			}
		}

		// Pass values to be callable by logic business to connect to target database based on information
		// from ops db

		go func() {
			err := pubMsg(uri)
			if err != nil {
				errc <- err
				glog.Error(err)
			}
		}()

		// Logic business function
		res, err := fn(request, c)

		// This section will not handle any error, allowed to print to stderr but should pass error without mutate.
		if err != nil {
			glog.Error(err)
			return nil, err
		}

		tx, err := r.Cli.Begin()
		defer func() {
			switch err {
			case nil:
				err = tx.Commit()
			default:
				tx.Rollback()
			}
		}()

		if err != nil {
			errString := errToString(err)
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: &errString,
			}
		}

		// Write into share_instance database info about:
		// instance parameters about json string.

		// No checking on request instance and status since not exist
		if id == request.InstanceID && status == statusDeleted {
			_, err := tx.Exec(`UPDATE share_instance SET instance_status = $1, delete_instance_time = $2 
	where instances_id = $3`, statusExist, time.Now(), request.InstanceID)

			// Confirm rows get updated. Return response Exists:true when at least 1 row get updated.
			if err != nil {
				errString := errToString(err)
				return nil, osb.HTTPStatusCodeError{
					StatusCode:   http.StatusInternalServerError,
					ErrorMessage: &errString,
				}
			}

		} else {

			// Not set instance_parameters
			_, err = tx.Exec(`INSERT INTO share_instance (database_pid, instances_id, plan_id, plan_name, org_id, spaces_id,
	create_instance_time,instance_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				pid, request.InstanceID, request.PlanID, planName, request.OrganizationGUID, request.SpaceGUID, time.Now(), statusExist)
			if err != nil {
				errString := errToString(err)
				return nil, osb.HTTPStatusCodeError{
					StatusCode:   http.StatusInternalServerError,
					ErrorMessage: &errString,
				}
			}
		}

		select {
		case <-errc:
			return nil, <-errc
		default:
			return res, nil
		}
	}
}

// OnDeprovision validates and record all Deprovision ops log then store into ops db
func (r *Recorder) OnDeprovision(fn ObjectDeprovision) ObjectDeprovision {
	return func(request *osb.DeprovisionRequest, c *broker.RequestContext) (*broker.DeprovisionResponse, error) {

		var id, status string
		errc := make(chan error)

		// Check to see if instance already exists
		r.Cli.QueryRow(`SELECT instances_id, instance_status FROM share_instance where instances_id = $1`,
			request.InstanceID).Scan(&id, &status)

		if id == request.InstanceID && status == statusDeleted || id != request.InstanceID {
			errString := errToString(fmt.Errorf(InstanceIDNotFound))
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusNotFound,
				ErrorMessage: &errString,
			}
		}

		var uri *string
		// Get credential and ip from ops DB
		// Query database target information for the service
		if e := r.Cli.QueryRow(`SELECT uri FROM credentials where shared_use = true LIMIT 1`).Scan(&uri); e != nil {
			errString := errToString(e)
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusConflict,
				ErrorMessage: &errString,
			}
		}

		// Pass values to be callable by logic business to connect to target database based on information
		// from ops db

		go func() {
			err := pubMsg(uri)
			if err != nil {
				errc <- err
				glog.Error(err)
			}
		}()

		res, err := fn(request, c)

		// This section will not handle any error, allowed to print to stderr but should pass error without mutate.
		if err != nil {
			glog.Error(err)
			return nil, err
		}

		tx, err := r.Cli.Begin()
		defer func() {
			switch err {
			case nil:
				err = tx.Commit()
			default:
				tx.Rollback()
			}
		}()

		if err != nil {
			errString := errToString(err)
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusInternalServerError,
				ErrorMessage: &errString,
			}
		}

		if id == request.InstanceID && status == statusExist {

			// Update rows
			_, err := r.Cli.Exec(`UPDATE share_instance set instance_status=$1, delete_instance_time=$2 where instances_id=$3`,
				statusDeleted, time.Now(), request.InstanceID)

			// Confirm rows get updated. Return response Exists:true when at least 1 row get updated.
			if err != nil {
				errString := errToString(err)
				return nil, osb.HTTPStatusCodeError{
					StatusCode:   http.StatusInternalServerError,
					ErrorMessage: &errString,
				}
			}
		}

		select {
		case <-errc:
			return nil, <-errc
		default:
			return res, nil
		}
	}
}

// OnBind validates and record all Bind ops log then store into ops db
func (r *Recorder) OnBind(fn ObjectBind) ObjectBind {
	return func(request *osb.BindRequest, c *broker.RequestContext) (*broker.BindResponse, error) {

		var id, status string
		var pid *string
		var bindID, bindStatus string
		errc := make(chan error)

		// Check to see if instance already exists
		r.Cli.QueryRow(`SELECT share_instance_pid, instances_id, instance_status FROM share_instance where instances_id = $1`,
			request.InstanceID).Scan(&pid, &id, &status)

		if id == request.InstanceID && status == statusDeleted || id != request.InstanceID {
			errString := errToString(fmt.Errorf(InstanceIDNotFound))
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusGone,
				ErrorMessage: &errString,
			}
		}

		// Check to see if bind already exists
		r.Cli.QueryRow(`SELECT binding_id, binding_status FROM share_binding where binding_id = $1`,
			request.BindingID).Scan(&bindID, &bindStatus)
		if bindID == request.BindingID && bindStatus == statusExist {
			errString := errToString(fmt.Errorf("BindingID in use"))
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusConflict,
				ErrorMessage: &errString,
			}
		}

		var uri *string
		// Get credential and ip from ops DB
		// Query database target information for the service
		if e := r.Cli.QueryRow(`SELECT  uri FROM credentials where shared_use = true LIMIT 1`).Scan(&uri); e != nil {
			errString := errToString(e)
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusConflict,
				ErrorMessage: &errString,
			}
		}

		// Pass values to be callable by logic business to connect to target database based on information
		// from ops db

		go func() {
			err := pubMsg(uri)
			if err != nil {
				errc <- err
				glog.Error(err)
			}
		}()

		res, err := fn(request, c)

		// This section will not handle any error, allowed to print to stderr but should pass error without mutate.
		if err != nil {
			glog.Error(err)
			return nil, err
		}

		tx, err := r.Cli.Begin()
		defer func() {
			switch err {
			case nil:
				err = tx.Commit()
			default:
				tx.Rollback()
			}
		}()

		if bindID == request.BindingID && bindStatus == statusDeleted {

			// Update rows
			_, err := r.Cli.Exec(`UPDATE share_binding set binding_status=$1, binding_time=$2 where binding_id=$3`,
				statusExist, time.Now(), request.BindingID)

			// Confirm rows get updated. Return response Exists:true when at least 1 row get updated.
			if err != nil {
				errString := errToString(err)
				return nil, osb.HTTPStatusCodeError{
					StatusCode:   http.StatusInternalServerError,
					ErrorMessage: &errString,
				}
			}
		} else {
			// Not set instance_parameters
			_, err := r.Cli.Exec(`INSERT INTO share_binding (share_instance_pid, binding_id, binding_time, binding_status) VALUES($1, $2, $3, $4)`,
				pid, request.BindingID, time.Now(), statusExist)
			if err != nil {
				errString := errToString(fmt.Errorf(InstanceIDNotFound))
				return nil, osb.HTTPStatusCodeError{
					StatusCode:   http.StatusInternalServerError,
					ErrorMessage: &errString,
				}
			}
		}

		select {
		case <-errc:
			return nil, <-errc
		default:
			return res, nil
		}
	}
}

// OnUnbind validates and record all Unbind ops log then store into ops db
func (r *Recorder) OnUnbind(fn ObjectUnbind) ObjectUnbind {
	return func(request *osb.UnbindRequest, c *broker.RequestContext) (*broker.UnbindResponse, error) {

		var id, status string
		var bindID, bindStatus string
		errc := make(chan error)

		// Check to see if instance already exists
		r.Cli.QueryRow(`SELECT instances_id, instance_status FROM share_instance where instances_id = $1`,
			request.InstanceID).Scan(&id, &status)

		if id == request.InstanceID && status == statusDeleted || id != request.InstanceID {
			glog.Error(InstanceIDNotFound)

			// It should pass by design
			// return nil, osb.HTTPStatusCodeError{
			// 	StatusCode:   http.StatusNotFound,
			// 	ErrorMessage: &errString,
			// }
			return nil, nil
		}

		// Check to see if bind already exists
		r.Cli.QueryRow(`SELECT binding_id, binding_status FROM share_binding where binding_id = $1`,
			request.BindingID).Scan(&bindID, &bindStatus)

		if bindID == request.BindingID && bindStatus == statusDeleted {
			errToString(fmt.Errorf("Already Unbinded"))

			// It should pass by design
			// errString := fmt.Sprint("Already Unbinded")
			// glog.Error(errString)

			// return nil, osb.HTTPStatusCodeError{
			// 	StatusCode:   http.StatusConflict,
			// 	ErrorMessage: &errString,
			// }
			return nil, nil
		}

		var uri *string
		// Get credential and ip from ops DB
		// Query database target information for the service
		if e := r.Cli.QueryRow(`SELECT uri FROM credentials where shared_use = true LIMIT 1`).Scan(&uri); e != nil {
			errString := errToString(e)
			return nil, osb.HTTPStatusCodeError{
				StatusCode:   http.StatusConflict,
				ErrorMessage: &errString,
			}
		}

		// Pass values to be callable by logic business to connect to target database based on information
		// from ops db

		go func() {
			err := pubMsg(uri)
			if err != nil {
				errc <- err
				glog.Error(err)
			}
		}()

		res, err := fn(request, c)

		// This section will not handle any error, allowed to print to stderr but should pass error without mutate.
		if err != nil {
			glog.Error(err)
			return nil, err
		}

		tx, err := r.Cli.Begin()
		defer func() {
			switch err {
			case nil:
				err = tx.Commit()
			default:
				tx.Rollback()
			}
		}()

		if bindID == request.BindingID && bindStatus == statusExist {
			_, err := r.Cli.Exec(`UPDATE share_binding set binding_status=$1, unbinding_time=$2 where binding_id=$3`,
				statusDeleted, time.Now(), request.BindingID)
			if err != nil {
				errToString(err)

				// It should pass by design
				// errString := err.Error()
				// glog.Error(err)
				// return nil, osb.HTTPStatusCodeError{
				// 	StatusCode:   http.StatusInternalServerError,
				// 	ErrorMessage: &errString,
				// }
				return nil, nil
			}

		}

		select {
		case <-errc:
			return nil, <-errc
		default:
			return res, nil
		}
	}
}

func createURI(DatabaseName string) string {
	var cred string

	host1 := os.Getenv("PG_HOST1")
	port1 := os.Getenv("PG_PORT1")

	username := os.Getenv("PG_USERNAME")
	password := os.Getenv("PG_PASSWORD")

	if username != "" && password != "" {
		cred = fmt.Sprintf("%s:%s@", username, password)
	}
	return fmt.Sprintf("postgres://%s%s:%s/%s?sslmode=disable", cred, host1, port1, DatabaseName)
}

func errToString(e error) string {
	glog.Error(e)
	return e.Error()
}
