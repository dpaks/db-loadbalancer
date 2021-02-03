package mydb

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// goworkers is a workerpool implementation
	// that I wrote sometime back
	"github.com/dpaks/goworkers"
)

const (
	// heartbeatPeriod specifies the interval in seconds
	// for a reader DB ping
	heartbeatPeriod time.Duration = 1 * time.Second
	// ping timeout for DB liveness check
	heartbeatTimeout time.Duration = time.Second
	// number of goworkers
	numWorkers uint32 = 50
)

// DB is a loadbalancing logical database that works on top of MySQL
// that intelligently routes write requests to the master DB and read requests
// across various read DBs.
type DB struct {
	// master DB
	master *sql.DB
	// readReplicas stores the read replicas
	readReplicas map[uint32]*sql.DB
	// readReplicasCount stores the count of readReplicas
	readReplicasCount uint32
	// count will have a sufficient and a practical max value assuming
	// the same max read replicas. This value is used for round robin algo
	count uint32
	// embedded mutex to control access to readReplicas resource
	sync.RWMutex
	// connInfo stores the information required to restart a connection
	// for a read replica
	connInfo map[uint32]string
	// name of the DB driver for the purpose of reconnection
	driverName string
	// stores the sick replicas
	sick map[uint32]struct{}
	// mutex for 'sick' resource
	sickMu sync.RWMutex
	// stores the resurrected replicas
	resurrect map[uint32]*sql.DB
	// mutex for 'resurrect' resource
	resurrectMu sync.RWMutex
	// an unbuffered channel to signal termination of health check go routine
	killHealthCheck chan struct{}
}

// checkBeat checks if a DB is reachable
func checkBeat(db *sql.DB) (err error) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()

	return db.PingContext(ctx)
}

// heartbeatChecker monitors the health of reader DBs every heartbeatPeriod seconds.
// This is not a fool-proof method to prevent DB connection issues but this will
// exclude the DBs in maintenance to appear in readReplicaRR
func (db *DB) heartbeatChecker() {
	opts := goworkers.Options{Workers: numWorkers}
	gw := goworkers.New(opts)
	defer gw.Stop(false)

	ticker := time.NewTicker(heartbeatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-db.killHealthCheck:
			return
		case <-ticker.C:
			db.RLock()
			for i, rR := range db.readReplicas {
				idx := uint32(i)
				readReplica := rR
				// if a DB is unreachable, check if it needs to be quarantined
				if err := checkBeat(readReplica); err != nil {
					gw.Submit(func() {
						// attempt reconnection
						_, err := db.reconnectDB(idx, readReplica)
						// reconnect failure
						// quarantine if not already
						if err != nil {
							db.quarantineReadReplica(idx)
						}
					})
				}
			}
			db.RUnlock()
			gw.Wait(false)

			db.resurrectMu.Lock()
			for idx, readReplica := range db.resurrect {
				db.Lock()
				// replace the old reader with the resurrected one
				db.readReplicas[idx] = readReplica
				db.Unlock()
				delete(db.resurrect, idx)
			}
			db.resurrectMu.Unlock()
		}
	}
}

// reconnectDB attempts to reconnect to a DB
func (db *DB) reconnectDB(idx uint32, oldDB *sql.DB) (newDB *sql.DB, err error) {
	dsn, ok := db.connInfo[idx]
	if !ok {
		err = fmt.Errorf("connection info not found")
		return
	}

	newDB, err = sql.Open(db.driverName, dsn)
	if err != nil {
		return nil, err
	}

	if err := checkBeat(newDB); err != nil {
		return nil, err
	}

	// mark the reader for resurrection since it is now live
	db.resurrectMu.Lock()
	db.resurrect[idx] = newDB
	db.resurrectMu.Unlock()

	db.unQuarantineReadReplica(idx)

	// close the older DB
	oldDB.Close()

	return
}

// Open opens a database specified by its database driver name and a
// semicolon separated list of driver-specific data source names
// with the first one being the master
// Open("cloudMysqlDriverName", ":memory:;:memory:;:memory:")
func Open(driverName, dataSourceNames string) (db *DB, err error) {
	dsns := strings.Split(dataSourceNames, ";")
	if len(dsns) < 2 {
		return nil, fmt.Errorf("At least one master and worker DB are required")
	}

	db = &DB{
		driverName: driverName,
		connInfo:   make(map[uint32]string),
		sick:       make(map[uint32]struct{}),
		resurrect:  make(map[uint32]*sql.DB),
	}

	master, err := sql.Open(driverName, dsns[0])
	if err != nil {
		return nil, err
	}

	var workers []*sql.DB
	for i, dsn := range dsns[1:] {
		worker, err := sql.Open(driverName, dsn)
		if err != nil {
			return nil, err
		}

		db.connInfo[uint32(i)] = dsn
		workers = append(workers, worker)
	}

	return db.newDB(master, workers...)
}

// newDB returns a DB object that automatically routes read/write requests
// across master/worker DBs.
func (db *DB) newDB(master *sql.DB, workers ...*sql.DB) (*DB, error) {
	if err := master.Ping(); err != nil {
		return nil, fmt.Errorf("Master DB is not reachable")
	}

	db.master = master

	db.readReplicas = make(map[uint32]*sql.DB)
	for i, readReplica := range workers {
		if err := readReplica.Ping(); err != nil {
			return nil, fmt.Errorf("ReadReplica DB %d is not reachable", i+1)
		}

		db.readReplicas[uint32(i)] = readReplica
	}

	db.readReplicasCount = uint32(len(db.readReplicas))
	if db.readReplicasCount == 0 {
		return nil, fmt.Errorf("At least one worker DB is required")
	} else if db.readReplicasCount > math.MaxUint32 {
		return nil, fmt.Errorf("Number of worker DBs cannot be more than %d", math.MaxUint32)
	}

	go db.heartbeatChecker()

	return db, nil
}

// readReplicaRR returns a live reader DB
// on a round robin basis
func (db *DB) readReplicaRR() (uint32, *sql.DB) {
	if db.readReplicasCount == 1 {
		db.RLock()
		defer db.RUnlock()
		return 0, db.readReplicas[0]
	}

	// db.count may overflow and start from 0
	idx := atomic.AddUint32(&db.count, 1) % db.readReplicasCount
	db.RLock()
	readReplica := db.readReplicas[idx]
	db.RUnlock()

	// if the readReplica is not sick, return it
	db.sickMu.RLock()
	_, sick := db.sick[idx]
	db.sickMu.RUnlock()
	if !sick {
		return idx, readReplica
	}

	// return the next readReplica
	idx = atomic.AddUint32(&db.count, 1) % db.readReplicasCount
	db.RLock()
	defer db.RUnlock()
	return idx, db.readReplicas[idx]
}

// Ping checks the health of master and reader DBs
func (db *DB) Ping() error {
	if err := db.master.Ping(); err != nil {
		return err
	}

	db.RLock()
	defer db.RUnlock()
	for _, readReplica := range db.readReplicas {
		if err := readReplica.Ping(); err != nil {
			return err
		}
	}

	return nil
}

// PingContext checks the health of master and reader DBs
func (db *DB) PingContext(ctx context.Context) error {
	if err := db.master.PingContext(ctx); err != nil {
		return err
	}

	db.RLock()
	defer db.RUnlock()
	for _, readReplica := range db.readReplicas {
		if err := readReplica.PingContext(ctx); err != nil {
			return err
		}
	}

	return nil
}

// if a DB is reachable and in quarantine, remove it from sick list
func (db *DB) unQuarantineReadReplica(idx uint32) {
	db.sickMu.RLock()
	_, present := db.sick[idx]
	db.sickMu.RUnlock()
	if !present {
		return
	}

	// perform removal only if present in sick list
	// This minimises lock contention
	db.sickMu.Lock()
	delete(db.sick, idx)
	db.sickMu.Unlock()
}

// if a DB is unreachable, add it to sick list
func (db *DB) quarantineReadReplica(idx uint32) {
	db.sickMu.RLock()
	_, present := db.sick[idx]
	db.sickMu.RUnlock()
	if present {
		return
	}

	// perform addition only if absent in sick list
	// This minimises lock contention
	db.sickMu.Lock()
	db.sick[idx] = struct{}{}
	db.sickMu.Unlock()
}

// Query executes a query that returns rows. The request will be processed by an endpoint
// chosen as per roundrobin algorithm. If a network error is returned,
// another endpoint is chosen. At a time, only one read replica will be down.
func (db *DB) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	idx, readReplica := db.readReplicaRR()
	rows, err = readReplica.Query(query, args...)
	// If it is a connection issue with the target, then try another endpoint.
	// Currently, this is an overkill. Ideally, catch all errors related to network.
	if err != nil {
		if err := checkBeat(readReplica); err != nil {
			// proactively quarantine the down readReplica
			go db.quarantineReadReplica(idx)
			_, readReplica := db.readReplicaRR()
			return readReplica.Query(query, args...)
		}
	}

	// if the error is not related to network issue,
	// return the original error
	return
}

// QueryContext behaves like Query but with context
func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	idx, readReplica := db.readReplicaRR()
	rows, err = readReplica.QueryContext(ctx, query, args...)
	// If it is a connection issue with the target, then try another endpoint.
	// Currently, this is an overkill. Ideally, catch all errors related to network.
	if err != nil {
		if err := checkBeat(readReplica); err != nil {
			// proactively quarantine the down readReplica
			go db.quarantineReadReplica(idx)
			_, readReplica := db.readReplicaRR()
			return readReplica.QueryContext(ctx, query, args...)
		}
	}

	// if the error is not related to network issue,
	// return the original error
	return
}

// QueryRow executes a query that is expected to return at most one row. The request will be
// processed by an endpoint chosen as per roundrobin algorithm.
func (db *DB) QueryRow(query string, args ...interface{}) (rows *sql.Row) {
	_, readReplica := db.readReplicaRR()
	return readReplica.QueryRow(query, args...)
}

// QueryRowContext behaves like QueryRow but with context
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Row) {
	_, readReplica := db.readReplicaRR()
	return readReplica.QueryRowContext(ctx, query, args...)
}

// Begin starts a transaction
func (db *DB) Begin() (*sql.Tx, error) {
	return db.master.Begin()
}

// BeginTx starts a transaction with context
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.master.BeginTx(ctx, opts)
}

// Close closes the database, cleans up resources and prevents new queries from starting
func (db *DB) Close() error {
	if db == nil {
		return nil
	}

	if db.master != nil {
		if err := db.master.Close(); err != nil {
			return err
		}
	}

	db.RLock()
	defer db.RUnlock()
	for _, readReplica := range db.readReplicas {
		if readReplica != nil {
			if err := readReplica.Close(); err != nil {
				return err
			}
		}
	}

	select {
	case db.killHealthCheck <- struct{}{}:
	default:
	}

	return nil
}

// Exec executes a query without returning any rows
func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.master.Exec(query, args...)
}

// ExecContext executes a query with context without returning any rows
func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.master.ExecContext(ctx, query, args...)
}

// Prepare creates a prepared statement for later queries or executions
func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.master.Prepare(query)
}

// PrepareContext creates a prepared statement with context for later queries or executions
func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.master.PrepareContext(ctx, query)
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.master.SetConnMaxLifetime(d)

	opts := goworkers.Options{Workers: numWorkers}
	gw := goworkers.New(opts)
	defer gw.Stop(false)

	db.RLock()
	defer db.RUnlock()
	for _, readReplica := range db.readReplicas {
		readReplica := readReplica
		gw.Submit(func() {
			readReplica.SetConnMaxLifetime(d)
		})
	}
}

// SetMaxIdleConns sets the maximum number of connections in the idle connection pool
func (db *DB) SetMaxIdleConns(n int) {
	db.master.SetMaxIdleConns(n)

	opts := goworkers.Options{Workers: numWorkers}
	gw := goworkers.New(opts)
	defer gw.Stop(false)

	db.RLock()
	defer db.RUnlock()
	for _, readReplica := range db.readReplicas {
		readReplica := readReplica
		gw.Submit(func() {
			readReplica.SetMaxIdleConns(n)
		})
	}
}

// SetMaxOpenConns sets the maximum number of open connections to the database
func (db *DB) SetMaxOpenConns(n int) {
	db.master.SetMaxOpenConns(n)

	opts := goworkers.Options{Workers: numWorkers}
	gw := goworkers.New(opts)
	defer gw.Stop(false)

	db.RLock()
	defer db.RUnlock()
	for _, readReplica := range db.readReplicas {
		readReplica := readReplica
		gw.Submit(func() {
			readReplica.SetMaxOpenConns(n)
		})
	}
}
