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

package apecloudpostgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cast"

	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/models"
	"github.com/apecloud/dbctl/engines/postgres"
)

type Manager struct {
	postgres.Manager
	memberAddrs  []string
	healthStatus *postgres.ConsensusMemberHealthStatus
}

var _ engines.DBManager = &Manager{}

var Mgr *Manager

func NewManager(properties engines.Properties) (engines.DBManager, error) {
	Mgr = &Manager{}

	baseManager, err := postgres.NewManager(properties)
	if err != nil {
		return nil, errors.Errorf("new base manager failed, err: %v", err)
	}

	Mgr.Manager = *baseManager.(*postgres.Manager)
	return Mgr, nil
}

func (mgr *Manager) IsLeaderWithHost(ctx context.Context, host string) (bool, error) {
	role, err := mgr.GetMemberRoleWithHost(ctx, host)
	if err != nil {
		return false, errors.Errorf("check is leader with host:%s failed, err:%v", host, err)
	}

	return role == strings.ToLower(models.LEADER), nil
}

func (mgr *Manager) IsDBStartupReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if mgr.DBStartupReady {
		return true
	}

	if !mgr.IsPgReady(ctx) {
		return false
	}

	if !mgr.isConsensusReadyUp(ctx) {
		return false
	}

	mgr.DBStartupReady = true
	mgr.Logger.Info("DB startup ready")
	return true
}

func (mgr *Manager) isConsensusReadyUp(ctx context.Context) bool {
	sql := `SELECT extname FROM pg_extension WHERE extname = 'consensus_monitor';`
	resp, err := mgr.Query(ctx, sql)
	if err != nil {
		mgr.Logger.Error(err, fmt.Sprintf("query sql:%s failed", sql))
		return false
	}

	resMap, err := postgres.ParseQuery(string(resp))
	if err != nil {
		mgr.Logger.Error(err, fmt.Sprintf("parse query response:%s failed", string(resp)))
		return false
	}

	return resMap[0]["extname"] != nil
}

func (mgr *Manager) GetMemberRoleWithHost(ctx context.Context, host string) (string, error) {
	sql := `select role from consensus_member_status;`

	resp, err := mgr.QueryWithHost(ctx, sql, host)
	if err != nil {
		return "", err
	}

	resMap, err := postgres.ParseQuery(string(resp))
	if err != nil {
		return "", err
	}

	return strings.ToLower(cast.ToString(resMap[0]["role"])), nil
}
