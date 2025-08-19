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

package engines

import (
	"context"

	"github.com/go-logr/logr"
	"github.com/pkg/errors"

	"github.com/apecloud/dbctl/constant"
	"github.com/apecloud/dbctl/engines/models"
)

type DBManagerBase struct {
	CurrentMemberName string
	CurrentMemberIP   string
	ClusterCompName   string
	Namespace         string
	DataDir           string
	Logger            logr.Logger
	DBStartupReady    bool
	IsLocked          bool
}

func NewDBManagerBase(logger logr.Logger) (*DBManagerBase, error) {
	currentMemberName := constant.GetPodName()
	if currentMemberName == "" {
		return nil, errors.New("pod name is not set")
	}

	mgr := DBManagerBase{
		CurrentMemberName: currentMemberName,
		CurrentMemberIP:   constant.GetPodIP(),
		ClusterCompName:   constant.GetClusterCompName(),
		Namespace:         constant.GetNamespace(),
		Logger:            logger,
	}
	return &mgr, nil
}

func (mgr *DBManagerBase) IsDBStartupReady() bool {
	return mgr.DBStartupReady
}

func (mgr *DBManagerBase) SetLogger(logger logr.Logger) {
	mgr.Logger = logger
}

func (mgr *DBManagerBase) GetReplicaRole(context.Context) (string, error) {
	return "", models.ErrNotImplemented
}

func (mgr *DBManagerBase) Exec(context.Context, string) (int64, error) {
	return 0, models.ErrNotImplemented
}

func (mgr *DBManagerBase) Query(context.Context, string) ([]byte, error) {
	return []byte{}, models.ErrNotImplemented
}

func (mgr *DBManagerBase) GetPort() (int, error) {
	return 0, models.ErrNotImplemented
}

func (mgr *DBManagerBase) Lock(context.Context, string) error {
	return models.ErrNotImplemented
}

func (mgr *DBManagerBase) Unlock(context.Context) error {
	return models.ErrNotImplemented
}

func (mgr *DBManagerBase) ShutDownWithWait() {
	mgr.Logger.Info("Override me if need")
}
