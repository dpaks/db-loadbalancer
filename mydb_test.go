package mydb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

func TestOpenOnlyMaster(t *testing.T) {
	db, err := Open("sqlite3", "file::memory:?cache=shared")
	assert.NotNil(t, err)
	assert.Empty(t, db)
}

func TestOpen(t *testing.T) {
	db, err := Open("sqlite3", "file::memory:?cache=shared;file::memory:?cache=shared")
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
}

func TestnewDBWithNoWorker(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)
	defer masterDB.Close()

	_, err = db.newDB(masterDB, nil)
	assert.NotNil(t, err)
}

func TestnewDBWithNoMaster(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	readerDB, _, err := sqlmock.New()
	assert.Nil(t, err)
	defer readerDB.Close()

	_, err = db.newDB(nil, readerDB)
	assert.NotNil(t, err)
}

func TestnewDBWithMasterReader(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	_, err = db.newDB(masterDB, readerDB)
	assert.Nil(t, err)
	defer db.Close()
}

func TestdbnewDBWithManyReaders(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDBs := []*sql.DB{}
	for i := 0; i < 1000; i++ {
		readerDB, _, err := sqlmock.New()
		assert.Nil(t, err)
		readerDBs = append(readerDBs, readerDB)
	}

	db, err = db.newDB(masterDB, readerDBs...)
	assert.Nil(t, err)
	defer db.Close()

	_, result := db.readReplicaRR()
	assert.NotNil(t, result)
}

func TestReadReplicaRoundRobin(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDB1, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDB2, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDB3, _, err := sqlmock.New()
	assert.Nil(t, err)

	db, err = db.newDB(masterDB, readerDB1, readerDB2, readerDB3)
	assert.Nil(t, err)
	defer db.Close()

	_, result := db.readReplicaRR()
	assert.Equal(t, result, readerDB2)

	_, result = db.readReplicaRR()
	assert.Equal(t, result, readerDB3)

	_, result = db.readReplicaRR()
	assert.Equal(t, result, readerDB1)

	_, result = db.readReplicaRR()
	assert.Equal(t, result, readerDB2)

	_, result = db.readReplicaRR()
	assert.Equal(t, result, readerDB3)

	_, result = db.readReplicaRR()
	assert.Equal(t, result, readerDB1)
}

func TestRoundRobinNextReader(t *testing.T) {
	masterDB, _, err := sqlmock.NewWithDSN("sqlmock_db_3d")
	assert.Nil(t, err)

	readerDB1, mock1, err := sqlmock.NewWithDSN("sqlmock_db_45")
	assert.Nil(t, err)

	readerDB2, mock2, err := sqlmock.NewWithDSN("sqlmock_db_55")
	assert.Nil(t, err)

	db, err := open("sqlmock", "", masterDB, readerDB1, readerDB2)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	db.connInfo[0] = "sqlmock_db_45"
	db.connInfo[1] = "sqlmock_db_55"

	err = db.Ping()
	assert.Nil(t, err)

	query := "SELECT name FROM user;"
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "Deepak")

	mock1.ExpectQuery(query).WillReturnRows(rows)
	mock2.ExpectQuery(query).WillReturnRows(rows)

	readerDB2.Close()
	time.Sleep(2 * heartbeatPeriod)
	db.sickMu.RLock()
	assert.Len(t, db.sick, 1)
	db.sickMu.RUnlock()

	db.Query(query)
	err = mock1.ExpectationsWereMet()
	assert.NoError(t, err)
	err = mock2.ExpectationsWereMet()
	assert.Error(t, err)
}

func TestPing(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	db, err = db.newDB(masterDB, readerDB)
	assert.Nil(t, err)
	defer db.Close()

	err = db.Ping()
	assert.Nil(t, err)
}

func TestPingError(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)
	defer masterDB.Close()

	readerDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	assert.Nil(t, err)
	defer readerDB.Close()

	mock.ExpectPing().WillReturnError(fmt.Errorf("ping failed for read replica"))

	_, err = db.newDB(masterDB, readerDB)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "ReadReplica DB 1 is not reachable")
}

func TestQuery(t *testing.T) {
	db := &DB{
		driverName: "mysql",
	}

	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDB, mock, err := sqlmock.New()
	assert.Nil(t, err)

	db, err = db.newDB(masterDB, readerDB)
	assert.Nil(t, err)
	defer db.Close()

	query := "SELECT name FROM user;"
	rows := sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "Deepak")

	mock.ExpectQuery(query).WillReturnRows(rows)

	name, err := db.Query(query)
	assert.NoError(t, err)
	assert.NotNil(t, name)
}

func open(driverName, dsn string, dbs ...*sql.DB) (db *DB, err error) {
	db = &DB{
		driverName:   driverName,
		connInfo:     make(map[uint32]string),
		sick:         make(map[uint32]struct{}),
		resurrect:    make(map[uint32]*sql.DB),
		readReplicas: make(map[uint32]*sql.DB),
	}

	master := dbs[0]

	var workers []*sql.DB
	for i, wdb := range dbs[1:] {
		db.connInfo[uint32(i)] = dsn
		workers = append(workers, wdb)
	}

	return db.newDB(master, workers...)
}

func TestQueryError(t *testing.T) {
	masterDB, _, err := sqlmock.New()
	assert.Nil(t, err)

	readerDB, mock, err := sqlmock.New()
	assert.Nil(t, err)

	db, err := open("sqlite3", "file::memory:?cache=shared", masterDB, readerDB)
	assert.Nil(t, err)
	defer db.Close()

	query := "SELECT name FROM user;"

	mock.ExpectQuery(query).WillReturnError(driver.ErrBadConn)

	name, err := db.Query(query)
	assert.Error(t, err)
	assert.Empty(t, name)
}

func TestMultiDBs(t *testing.T) {
	t.Parallel()
	var wg sync.WaitGroup

	masterDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	var readerDBs []*sql.DB
	for i := 0; i < 100; i++ {
		readerDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
		assert.Nil(t, err)
		readerDBs = append(readerDBs, readerDB)
	}

	var dbs []*sql.DB
	dbs = append(dbs, masterDB)
	dbs = append(dbs, readerDBs...)

	db, err := open("sqlite3", "file::memory:?cache=shared", dbs...)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	query := "CREATE TABLE IF NOT EXISTS user (name TEXT)"
	_, err = db.Exec(query)
	assert.NoError(t, err)

	query = "INSERT INTO user (name) VALUES ('deepak')"
	_, err = db.Exec(query)
	assert.NoError(t, err)

	fn := func() {
		defer wg.Done()
		for i := 0; i < len(readerDBs); i += 2 {
			readerDBs[i].Close()
		}

		time.Sleep(2 * heartbeatPeriod)

		q := "SELECT name FROM user;"
		name, err := db.Query(q)
		assert.NoError(t, err)
		assert.NotNil(t, name)
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go fn()
	}

	wg.Wait()
}

func TestQueryResultNoFailover(t *testing.T) {
	masterDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB1, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	db, err := open("sqlite3", "file::memory:?cache=shared", masterDB, readerDB1)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	readerDB1.Close()

	query := "CREATE TABLE IF NOT EXISTS user(name blob)"
	_, err = db.Exec(query)
	assert.NoError(t, err)

	query = "SELECT name FROM user;"
	name, err := db.Query(query)
	assert.Error(t, err)
	assert.Nil(t, name)
}

func TestQueryResultFailover(t *testing.T) {
	masterDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB1, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB2, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	db, err := open("sqlite3", "file::memory:?cache=shared", masterDB, readerDB1, readerDB2)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	readerDB2.Close()

	query := "CREATE TABLE IF NOT EXISTS user(name blob)"
	_, err = db.Exec(query)
	assert.NoError(t, err)

	query = "SELECT name FROM user;"
	name, err := db.Query(query)
	assert.NoError(t, err)
	assert.NotNil(t, name)
}

func TestMultiCallers(t *testing.T) {
	var wg sync.WaitGroup

	fn := func() {
		defer wg.Done()

		masterDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
		assert.Nil(t, err)

		readerDB1, err := sql.Open("sqlite3", "file::memory:?cache=shared")
		assert.Nil(t, err)

		readerDB2, err := sql.Open("sqlite3", "file::memory:?cache=shared")
		assert.Nil(t, err)

		db, err := open("sqlite3", "file::memory:?cache=shared", masterDB, readerDB1, readerDB2)
		assert.Nil(t, err)
		assert.NotEmpty(t, db)
		defer db.Close()

		readerDB2.Close()

		query := "CREATE TABLE IF NOT EXISTS user(name blob)"
		_, err = db.Exec(query)
		assert.NoError(t, err)

		query = "SELECT name FROM user;"
		name, err := db.Query(query)
		assert.NoError(t, err)
		assert.NotNil(t, name)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go fn()
	}

	wg.Wait()
}

func TestHeartbeatRecovery(t *testing.T) {
	masterDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB1, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB2, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	db, err := open("sqlite3", "file::memory:?cache=shared", masterDB, readerDB1, readerDB2)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	err = db.Ping()
	assert.Nil(t, err)

	readerDB2.Close()

	err = db.Ping()
	assert.NotNil(t, err)

	time.Sleep(2 * heartbeatPeriod)

	err = db.Ping()
	assert.Nil(t, err)
}

func TestHeartbeat(t *testing.T) {
	masterDB, _, err := sqlmock.NewWithDSN("sqlmock_db_3d")
	assert.Nil(t, err)

	readerDB1, _, err := sqlmock.NewWithDSN("sqlmock_db_4s")
	assert.Nil(t, err)

	readerDB2, mock, err := sqlmock.NewWithDSN("sqlmock_db_5x")
	assert.Nil(t, err)

	db, err := open("sqlmock", "", masterDB, readerDB1, readerDB2)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	db.connInfo[0] = "sqlmock_db_4s"
	db.connInfo[1] = "sqlmock_db_5x"

	err = db.Ping()
	assert.Nil(t, err)

	mock.ExpectClose().WillReturnError(fmt.Errorf("some error"))
	readerDB2.Close()
	time.Sleep(2 * heartbeatPeriod)
	db.sickMu.RLock()
	assert.Len(t, db.sick, 1)
	db.sickMu.RUnlock()
}

func TestSetConnMaxLifetime(t *testing.T) {
	masterDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB1, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB2, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	db, err := open("sqlite3", "file::memory:?cache=shared", masterDB, readerDB1, readerDB2)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	err = db.Ping()
	assert.Nil(t, err)

	db.SetConnMaxLifetime(10)

	err = db.Ping()
	assert.Nil(t, err)
}

func TestSetMaxOpenConns(t *testing.T) {
	masterDB, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB1, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	readerDB2, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	assert.Nil(t, err)

	db, err := open("sqlite3", "file::memory:?cache=shared", masterDB, readerDB1, readerDB2)
	assert.Nil(t, err)
	assert.NotEmpty(t, db)
	defer db.Close()

	err = db.Ping()
	assert.Nil(t, err)

	db.SetMaxOpenConns(10)

	err = db.Ping()
	assert.Nil(t, err)
}
