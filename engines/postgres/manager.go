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

package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/spf13/viper"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/engines"
)

type Manager struct {
	engines.DBManagerBase
	MajorVersion int
	Pool         PgxPoolIFace
	Proc         *process.Process
	Config       *Config
}

func NewManager() (engines.DBManager, error) {
	logger := ctrl.Log.WithName("PostgreSQL")
	config, err := NewConfig()
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config.pgxConfig)
	if err != nil {
		return nil, errors.Errorf("unable to ping the DB: %v", err)
	}

	managerBase, err := engines.NewDBManagerBase(logger)
	if err != nil {
		return nil, err
	}
	managerBase.DataDir = viper.GetString(PGDATA)

	mgr := &Manager{
		DBManagerBase: *managerBase,
		Pool:          pool,
		Config:        config,
		MajorVersion:  viper.GetInt(PGMAJOR),
	}

	return mgr, nil
}

func (mgr *Manager) IsPgReady(ctx context.Context) bool {
	err := mgr.Pool.Ping(ctx)
	if err != nil {
		mgr.Logger.Error(err, "DB is not ready, ping failed")
		return false
	}

	return true
}
