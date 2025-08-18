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

package oceanbase

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/mysql"
)

const (
	PRIMARY = "PRIMARY"
	STANDBY = "STANDBY"

	normalStatus = "NORMAL"
)

type Manager struct {
	mysql.Manager
	ReplicaTenant     string
	CompatibilityMode string
	MaxLag            int64
}

var _ engines.DBManager = &Manager{}

func NewManager(properties engines.Properties) (engines.DBManager, error) {
	logger := ctrl.Log.WithName("Oceanbase")
	config, err := NewConfig(properties)
	if err != nil {
		return nil, err
	}

	managerBase, err := engines.NewDBManagerBase(logger)
	if err != nil {
		return nil, err
	}

	db, err := config.GetLocalDBConn()
	if err != nil {
		return nil, errors.Wrap(err, "connect to Oceanbase failed")
	}

	mgr := &Manager{
		Manager: mysql.Manager{
			DBManagerBase: *managerBase,
			DB:            db,
		},
	}
	mgr.ReplicaTenant = viper.GetString("TENANT_NAME")
	if mgr.ReplicaTenant == "" {
		return nil, errors.New("replica tenant is not set")
	}
	return mgr, nil
}

func (mgr *Manager) GetCompatibilityMode(ctx context.Context) (string, error) {
	if mgr.CompatibilityMode != "" {
		return mgr.CompatibilityMode, nil
	}
	sql := fmt.Sprintf("SELECT COMPATIBILITY_MODE FROM oceanbase.DBA_OB_TENANTS where TENANT_NAME='%s'", mgr.ReplicaTenant)
	err := mgr.DB.QueryRowContext(ctx, sql).Scan(&mgr.CompatibilityMode)
	if err != nil {
		return "", errors.Wrap(err, "query compatibility mode failed")
	}
	return mgr.CompatibilityMode, nil
}

func (mgr *Manager) WriteCheck(ctx context.Context, db *sql.DB) error {
	writeSQL := fmt.Sprintf(`BEGIN;
CREATE DATABASE IF NOT EXISTS kubeblocks;
CREATE TABLE IF NOT EXISTS kubeblocks.kb_health_check(type INT, check_ts BIGINT, PRIMARY KEY(type));
INSERT INTO kubeblocks.kb_health_check VALUES(%d, UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE check_ts = UNIX_TIMESTAMP();
COMMIT;`, engines.CheckStatusType)
	opTimestamp, _ := mgr.GetOpTimestamp(ctx, db)
	if opTimestamp != 0 {
		// if op timestamp is not 0, it means the table is ready created
		writeSQL = fmt.Sprintf(`
		INSERT INTO kubeblocks.kb_health_check VALUES(%d, UNIX_TIMESTAMP()) ON DUPLICATE KEY UPDATE check_ts = UNIX_TIMESTAMP();
		`, engines.CheckStatusType)
	}
	_, err := db.ExecContext(ctx, writeSQL)
	if err != nil {
		return errors.Wrap(err, "Write check failed")
	}
	return nil
}

func (mgr *Manager) Demote(ctx context.Context) error {
	db := mgr.DB
	standbyTenant := "ALTER SYSTEM SWITCHOVER TO STANDBY TENANT = " + mgr.ReplicaTenant
	_, err := db.Exec(standbyTenant)
	if err != nil {
		return errors.Wrap(err, "standby primary tenant failed")
	}

	var tenantRole, roleStatus string
	queryTenant := fmt.Sprintf("SELECT TENANT_ROLE, SWITCHOVER_STATUS FROM oceanbase.DBA_OB_TENANTS where TENANT_NAME='%s'", mgr.ReplicaTenant)
	for {
		err := db.QueryRowContext(ctx, queryTenant).Scan(&tenantRole, &roleStatus)
		if err != nil {
			return errors.Wrap(err, "query tenant role failed")
		}

		if tenantRole == STANDBY && roleStatus == normalStatus {
			break
		}
		time.Sleep(time.Second)
	}

	return nil
}
