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

package vanillapostgres

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cast"

	"github.com/apecloud/dbctl/dcs"
	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/models"
	"github.com/apecloud/dbctl/engines/postgres"
)

type Manager struct {
	postgres.Manager
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

func (mgr *Manager) IsLeader(ctx context.Context, _ *dcs.Cluster) (bool, error) {
	isSet, isLeader := mgr.GetIsLeader()
	if isSet {
		return isLeader, nil
	}

	return mgr.IsLeaderWithHost(ctx, "")
}

func (mgr *Manager) IsLeaderWithHost(ctx context.Context, host string) (bool, error) {
	role, err := mgr.GetMemberRoleWithHost(ctx, host)
	if err != nil {
		return false, errors.Errorf("check is leader with host:%s failed, err:%v", host, err)
	}

	mgr.Logger.Info(fmt.Sprintf("get member:%s role:%s", host, role))
	return role == models.PRIMARY, nil
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

	mgr.DBStartupReady = true
	mgr.Logger.Info("DB startup ready")
	return true
}

func (mgr *Manager) GetMemberRoleWithHost(ctx context.Context, host string) (string, error) {
	sql := "select pg_is_in_recovery();"

	resp, err := mgr.QueryWithHost(ctx, sql, host)
	if err != nil {
		mgr.Logger.Error(err, "get member role failed")
		return "", err
	}

	result, err := postgres.ParseQuery(string(resp))
	if err != nil {
		mgr.Logger.Error(err, "parse member role failed")
		return "", err
	}

	if cast.ToBool(result[0]["pg_is_in_recovery"]) {
		return models.SECONDARY, nil
	} else {
		return models.PRIMARY, nil
	}
}

func (mgr *Manager) IsMemberHealthy(ctx context.Context, cluster *dcs.Cluster, member *dcs.Member) bool {
	var host string
	if member.Name != mgr.CurrentMemberName {
		host = cluster.GetMemberAddr(*member)
	}

	if cluster.Leader != nil && cluster.Leader.Name == member.Name {
		if !mgr.WriteCheck(ctx, host) {
			return false
		}
	}
	if !mgr.ReadCheck(ctx, host) {
		return false
	}

	return true
}

func (mgr *Manager) HasOtherHealthyMembers(ctx context.Context, cluster *dcs.Cluster, leader string) []*dcs.Member {
	members := make([]*dcs.Member, 0)

	for i, m := range cluster.Members {
		if m.Name != leader && mgr.IsMemberHealthy(ctx, cluster, &m) {
			members = append(members, &cluster.Members[i])
		}
	}

	return members
}
