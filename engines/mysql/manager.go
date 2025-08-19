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
	"time"

	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/engines"
)

type Manager struct {
	engines.DBManagerBase
	DB                           *sql.DB
	hostname                     string
	version                      string
	binlogFormat                 string
	logbinEnabled                bool
	logReplicationUpdatesEnabled bool
	slaveStatus                  RowMap
}

var _ engines.DBManager = &Manager{}

func NewManager() (engines.DBManager, error) {
	logger := ctrl.Log.WithName("MySQL")
	config, err := NewConfig()
	if err != nil {
		return nil, err
	}

	managerBase, err := engines.NewDBManagerBase(logger)
	if err != nil {
		return nil, err
	}

	db, err := config.GetLocalDBConn()
	if err != nil {
		return nil, errors.Wrap(err, "connect to MySQL")
	}

	mgr := &Manager{
		DBManagerBase: *managerBase,
		DB:            db,
	}

	return mgr, nil
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

func (mgr *Manager) ShutDownWithWait() {
	for _, db := range connectionPoolCache {
		_ = db.Close()
	}
	connectionPoolCache = make(map[string]*sql.DB)
}

func (mgr *Manager) IsReadonly(ctx context.Context) (bool, error) {
	var readonly bool
	err := mgr.DB.QueryRowContext(ctx, "select @@global.hostname, @@global.version, "+
		"@@global.read_only, @@global.binlog_format, @@global.log_bin, @@global.log_slave_updates").
		Scan(&mgr.hostname, &mgr.version, &readonly, &mgr.binlogFormat,
			&mgr.logbinEnabled, &mgr.logReplicationUpdatesEnabled)
	if err != nil {
		mgr.Logger.Info("Get global readonly failed", "error", err.Error())
		return false, err
	}
	return readonly, nil
}
