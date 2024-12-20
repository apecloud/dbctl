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

	"github.com/apecloud/dbctl/engines/models"
)

func (mgr *Manager) GetReplicaRole(ctx context.Context) (string, error) {
	return mgr.GetReplicaRoleFromDB(ctx)
}

func (mgr *Manager) GetReplicaRoleFromDB(ctx context.Context) (string, error) {
	slaveRunning, err := mgr.isSlaveRunning()
	if err != nil {
		return "", err
	}
	if slaveRunning {
		return models.SECONDARY, nil
	}

	hasSlave, err := mgr.hasSlaveHosts()
	if err != nil {
		return "", err
	}
	if hasSlave {
		return models.PRIMARY, nil
	}

	isReadonly, err := mgr.IsReadonly(ctx, nil, nil)
	if err != nil {
		return "", err
	}
	if isReadonly {
		// TODO: in case of diskFull lock, database will be set readonly,
		// how to deal with this situation
		return models.SECONDARY, nil
	}

	return models.PRIMARY, nil
}

func (mgr *Manager) isSlaveRunning() (bool, error) {
	var rowMap = mgr.slaveStatus

	if len(rowMap) == 0 {
		return false, nil
	}
	ioRunning := rowMap.GetString("Slave_IO_Running")
	sqlRunning := rowMap.GetString("Slave_SQL_Running")
	if ioRunning == "Yes" || sqlRunning == "Yes" {
		return true, nil
	}
	return false, nil
}

func (mgr *Manager) hasSlaveHosts() (bool, error) {
	sql := "show slave hosts"
	var rowMap RowMap

	err := QueryRowsMap(mgr.DB, sql, func(rMap RowMap) error {
		rowMap = rMap
		return nil
	})
	if err != nil {
		mgr.Logger.Info(sql+" failed", "error", err)
		return false, err
	}

	if len(rowMap) == 0 {
		return false, nil
	}

	return true, nil
}
