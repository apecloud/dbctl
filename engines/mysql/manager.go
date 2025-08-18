/*
Copyright (C) 2022-2023 ApeCloud Co., Ltd

This file is part of KubeBlocks project

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/engines"
)

type Manager struct {
	engines.DBManagerBase
	DB                           *sql.DB
	hostname                     string
	serverID                     uint
	version                      string
	binlogFormat                 string
	logbinEnabled                bool
	logReplicationUpdatesEnabled bool
	opTimestamp                  int64
	globalState                  map[string]string
	masterStatus                 RowMap
	slaveStatus                  RowMap
}

var _ engines.DBManager = &Manager{}

func NewManager(properties engines.Properties) (engines.DBManager, error) {
	logger := ctrl.Log.WithName("MySQL")
	config, err := NewConfig(properties)
	if err != nil {
		return nil, err
	}

	managerBase, err := engines.NewDBManagerBase(logger)
	if err != nil {
		return nil, err
	}

	serverID, err := engines.GetIndex(managerBase.CurrentMemberName)
	if err != nil {
		return nil, err
	}

	db, err := config.GetLocalDBConn()
	if err != nil {
		return nil, errors.Wrap(err, "connect to MySQL")
	}

	mgr := &Manager{
		DBManagerBase: *managerBase,
		serverID:      uint(serverID) + 1,
		DB:            db,
	}

	return mgr, nil
}

func (mgr *Manager) IsRunning() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// test if db is ready to connect or not
	err := mgr.DB.PingContext(ctx)
	if err != nil {
		var driverErr *mysql.MySQLError
		if errors.As(err, &driverErr) {
			// Now the error number is accessible directly
			if driverErr.Number == 1040 {
				mgr.Logger.Info("connect failed: Too many connections")
				return true
			}
		}
		mgr.Logger.Info("DB is not ready", "error", err)
		return false
	}

	return true
}

func (mgr *Manager) IsDBStartupReady() bool {
	if mgr.DBStartupReady {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// test if db is ready to connect or not
	err := mgr.DB.PingContext(ctx)
	if err != nil {
		mgr.Logger.Info("DB is not ready", "error", err)
		return false
	}

	mgr.DBStartupReady = true
	mgr.Logger.Info("DB startup ready")
	return true
}

func (mgr *Manager) GetSecondsBehindMaster(ctx context.Context) (int, error) {
	slaveStatus, err := mgr.GetSlaveStatus(ctx, mgr.DB)
	if err != nil {
		mgr.Logger.Info("show slave status failed", "error", err)
		return 0, err
	}
	if len(slaveStatus) == 0 {
		return 0, nil
	}
	secondsBehindMaster := slaveStatus.GetString("Seconds_Behind_Master")
	if secondsBehindMaster == "NULL" || secondsBehindMaster == "" {
		return 0, nil
	}
	return strconv.Atoi(secondsBehindMaster)
}

func (mgr *Manager) WriteCheck(ctx context.Context, db *sql.DB) error {
	writeSQL := fmt.Sprintf(`BEGIN;
CREATE DATABASE IF NOT EXISTS kubeblocks;
CREATE TABLE IF NOT EXISTS kubeblocks.kb_health_check(type INT, check_ts BIGINT, PRIMARY KEY(type));
INSERT INTO kubeblocks.kb_health_check VALUES(%d, UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE check_ts = UNIX_TIMESTAMP();
COMMIT;`, engines.CheckStatusType)
	_, err := db.ExecContext(ctx, writeSQL)
	if err != nil {
		mgr.Logger.Info(writeSQL+" executing failed", "error", err.Error())
		return err
	}
	return nil
}

func (mgr *Manager) ReadCheck(ctx context.Context, db *sql.DB) error {
	_, err := mgr.GetOpTimestamp(ctx, db)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// no healthy check records, return true
			return nil
		}
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && (mysqlErr.Number == 1049 || mysqlErr.Number == 1146) {
			// error 1049: database does not exists
			// error 1146: table does not exists
			// no healthy database, return true
			return nil
		}
		mgr.Logger.Info("Read check failed", "error", err)
		return err
	}

	return nil
}

func (mgr *Manager) GetOpTimestamp(ctx context.Context, db *sql.DB) (int64, error) {
	readSQL := fmt.Sprintf(`select check_ts from kubeblocks.kb_health_check where type=%d limit 1;`, engines.CheckStatusType)
	var opTimestamp int64
	err := db.QueryRowContext(ctx, readSQL).Scan(&opTimestamp)
	return opTimestamp, err
}

func (mgr *Manager) GetGlobalState(ctx context.Context, db *sql.DB) (map[string]string, error) {
	var hostname, serverUUID, gtidExecuted, gtidPurged, isReadonly, superReadonly string
	err := db.QueryRowContext(ctx, "select  @@global.hostname, @@global.server_uuid, @@global.gtid_executed, @@global.gtid_purged, @@global.read_only, @@global.super_read_only").
		Scan(&hostname, &serverUUID, &gtidExecuted, &gtidPurged, &isReadonly, &superReadonly)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"hostname":        hostname,
		"server_uuid":     serverUUID,
		"gtid_executed":   gtidExecuted,
		"gtid_purged":     gtidPurged,
		"read_only":       isReadonly,
		"super_read_only": superReadonly,
	}, nil
}

func (mgr *Manager) GetSlaveStatus(context.Context, *sql.DB) (RowMap, error) {
	sql := "show slave status"
	var rowMap RowMap

	err := QueryRowsMap(mgr.DB, sql, func(rMap RowMap) error {
		rowMap = rMap
		return nil
	})
	if err != nil {
		mgr.Logger.Info("executing "+sql+" failed", "error", err.Error())
		return nil, err
	}
	return rowMap, nil
}

func (mgr *Manager) GetMasterStatus(context.Context, *sql.DB) (RowMap, error) {
	sql := "show master status"
	var rowMap RowMap

	err := QueryRowsMap(mgr.DB, sql, func(rMap RowMap) error {
		rowMap = rMap
		return nil
	})
	if err != nil {
		mgr.Logger.Info("executing "+sql+" failed", "error", err.Error())
		return nil, err
	}
	return rowMap, nil
}

func (mgr *Manager) GetVersion(ctx context.Context) (string, error) {
	if mgr.version != "" {
		return mgr.version, nil
	}
	err := mgr.DB.QueryRowContext(ctx, "select version()").Scan(&mgr.version)
	if err != nil {
		return "", errors.Wrap(err, "Get version failed")
	}
	return mgr.version, nil
}

func (mgr *Manager) EnsureServerID(ctx context.Context) (bool, error) {
	var serverID uint
	err := mgr.DB.QueryRowContext(ctx, "select @@global.server_id").Scan(&serverID)
	if err != nil {
		mgr.Logger.Info("Get global server id failed", "error", err)
		return false, err
	}
	if serverID == mgr.serverID {
		return true, nil
	}
	mgr.Logger.Info("Set global server id", "server_id", serverID)

	setServerID := fmt.Sprintf(`set global server_id = %d`, mgr.serverID)
	mgr.Logger.Info("Set global server id", "server-id", setServerID)
	_, err = mgr.DB.Exec(setServerID)
	if err != nil {
		mgr.Logger.Info("set server id failed", "error", err)
		return false, err
	}

	return true, nil
}

func (mgr *Manager) EnableSemiSyncIfNeed(ctx context.Context) error {
	var status string
	err := mgr.DB.QueryRowContext(ctx, "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS "+
		"WHERE PLUGIN_NAME ='rpl_semi_sync_source';").Scan(&status)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		mgr.Logger.Info("Get rpl_semi_sync_source plugin status failed", "error", err.Error())
		return err
	}

	// In MySQL 8.0, semi-sync configuration options should not be specified in my.cnf,
	// as this may cause the database initialization process to fail:
	//    [Warning] [MY-013501] [Server] Ignoring --plugin-load[_add] list as the server is running with --initialize(-insecure).
	//    [ERROR] [MY-000067] [Server] unknown variable 'rpl_semi_sync_master_enabled=1'.
	if status == "ACTIVE" {
		setSourceEnable := "SET GLOBAL rpl_semi_sync_source_enabled = 1;" +
			"SET GLOBAL rpl_semi_sync_source_timeout = 100000;"
		_, err = mgr.DB.Exec(setSourceEnable)
		if err != nil {
			mgr.Logger.Info(setSourceEnable+" execute failed", "error", err.Error())
			return err
		}
	}

	err = mgr.DB.QueryRowContext(ctx, "SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS "+
		"WHERE PLUGIN_NAME ='rpl_semi_sync_replica';").Scan(&status)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		mgr.Logger.Info("Get rpl_semi_sync_replica plugin status failed", "error", err.Error())
		return err
	}

	if status == "ACTIVE" {
		setSourceEnable := "SET GLOBAL rpl_semi_sync_replica_enabled = 1;"
		_, err = mgr.DB.Exec(setSourceEnable)
		if err != nil {
			mgr.Logger.Info(setSourceEnable+" execute failed", "error", err.Error())
			return err
		}
	}
	return nil
}

func (mgr *Manager) Demote(context.Context) error {
	setReadOnly := `set global read_only=on;set global super_read_only=on;`

	_, err := mgr.DB.Exec(setReadOnly)
	if err != nil {
		mgr.Logger.Info("demote failed", "error", err.Error())
		return err
	}
	return nil
}

func (mgr *Manager) isRecoveryConfOutdated(leader string) bool {
	var rowMap = mgr.slaveStatus

	if len(rowMap) == 0 {
		return true
	}

	ioRunning := rowMap.GetString("Slave_IO_Running")
	sqlRunning := rowMap.GetString("Slave_SQL_Running")
	if ioRunning == "No" || sqlRunning == "No" {
		mgr.Logger.Info("slave status error", "status", rowMap)
		return true
	}

	masterHost := rowMap.GetString("Master_Host")
	return !strings.HasPrefix(masterHost, leader)
}

func (mgr *Manager) IsRootCreated(context.Context) (bool, error) {
	return true, nil
}

func (mgr *Manager) CreateRoot(context.Context) error {
	return nil
}

func (mgr *Manager) Lock(context.Context, string) error {
	setReadOnly := `set global read_only=on;`

	_, err := mgr.DB.Exec(setReadOnly)
	if err != nil {
		mgr.Logger.Info("Lock failed", "error", err.Error())
		return err
	}
	mgr.IsLocked = true
	return nil
}

func (mgr *Manager) Unlock(context.Context) error {
	setReadOnlyOff := `set global read_only=off;`

	_, err := mgr.DB.Exec(setReadOnlyOff)
	if err != nil {
		mgr.Logger.Info("Unlock failed", "error", err.Error())
		return err
	}
	mgr.IsLocked = false
	return nil
}

func (mgr *Manager) ShutDownWithWait() {
	for _, db := range connectionPoolCache {
		_ = db.Close()
	}
	connectionPoolCache = make(map[string]*sql.DB)
}
