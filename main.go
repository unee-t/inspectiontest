package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
)

type handler struct {
	DSN string // e.g. "bugzilla:secret@tcp(auroradb.dev.unee-t.com:3306)/bugzilla?multiStatements=true&sql_mode=TRADITIONAL"
	db  *sql.DB
}

func main() {

	h, err := New()
	if err != nil {
		log.WithError(err).Fatal("error setting configuration")
		return
	}

	defer h.db.Close()

	addr := ":" + os.Getenv("PORT")
	app := mux.NewRouter()
	app.HandleFunc("/", h.ping).Methods("GET")
	app.HandleFunc("/dbtimeout", h.dbtimeout).Methods("GET")
	app.HandleFunc("/time", h.timeYourTable).Methods("GET")
	app.HandleFunc("/gotimeout", gotimeout).Methods("GET")
	if err := http.ListenAndServe(addr, app); err != nil {
		log.WithError(err).Fatal("error listening")
	}

}

// New setups the configuration assuming various parameters have been setup in the AWS account
func New() (h handler, err error) {

	h = handler{
		DSN: fmt.Sprintf("%s:%s@tcp(%s:3306)/test?multiStatements=true&sql_mode=TRADITIONAL",
			os.Getenv("MYSQL_USER"),
			os.Getenv("MYSQL_PASS"),
			os.Getenv("MYSQL_HOST"),
		),
	}

	h.db, err = sql.Open("mysql", h.DSN)
	if err != nil {
		log.WithError(err).Fatal("error opening database")
		return
	}

	return

}

func (h handler) timeYourTable(w http.ResponseWriter, r *http.Request) {
	log.Info("with TX ... NO timeout")
	tx, err := h.db.Begin()
	if err != nil {
		log.WithError(err).Error("failed to start transaction")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Info("Executing")
	res, execErr := tx.Exec(fmt.Sprintf("UPDATE your_table SET val = %d WHERE id = 1; SELECT SLEEP(5.5);", time.Now().Unix()))
	if execErr != nil {
		log.WithError(err).Error("rolling back")
		err = tx.Rollback()
		if err != nil {
			log.WithError(err).Error("failed to roll back")
		}
	}
	log.Info("About to commit")
	if err := tx.Commit(); err != nil {
		log.WithError(err).Error("failed to commit")
	}
	count, err := res.RowsAffected()
	if err != nil {
		log.WithError(err).Error("failed to figure out how many rows were affected")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	fmt.Fprintf(w, fmt.Sprintf("OK %d", count))
}

func (h handler) dbtimeout(w http.ResponseWriter, r *http.Request) {
	_, err := h.db.Exec(`SELECT SLEEP(5.5);`)
	if err != nil {
		log.WithError(err).Error("failed to ping database")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	fmt.Fprintf(w, "OK")
}

func (h handler) ping(w http.ResponseWriter, r *http.Request) {
	err := h.db.Ping()
	if err != nil {
		log.WithError(err).Error("failed to ping database")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	fmt.Fprintf(w, "OK")
}

func gotimeout(w http.ResponseWriter, r *http.Request) {
	time.Sleep(5 * time.Second)
	fmt.Fprintf(w, "OK")
}
