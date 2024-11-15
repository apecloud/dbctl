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
	"github.com/spf13/viper"

	"github.com/apecloud/dbctl/dcs"
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

// IsMemberHealthy firstly get the leader's connection pool,
// because only leader can get the cluster healthy view
func (mgr *Manager) IsMemberHealthy(ctx context.Context, cluster *dcs.Cluster, member *dcs.Member) bool {
	healthStatus, err := mgr.getMemberHealthStatus(ctx, cluster, member)
	if errors.Is(err, postgres.ClusterHasNoLeader) {
		mgr.Logger.Info("cluster has no leader, will compete the leader lock")
		return true
	} else if err != nil {
		mgr.Logger.Error(err, "check member healthy failed")
		return false
	}

	return healthStatus.Connected
}

func (mgr *Manager) getMemberHealthStatus(ctx context.Context, cluster *dcs.Cluster, member *dcs.Member) (*postgres.ConsensusMemberHealthStatus, error) {
	if mgr.DBState != nil && mgr.healthStatus != nil {
		return mgr.healthStatus, nil
	}
	res := &postgres.ConsensusMemberHealthStatus{}

	IPPort := mgr.Config.GetConsensusIPPort(cluster, member.Name)
	sql := fmt.Sprintf(`select connected, log_delay_num from consensus_cluster_health where ip_port = '%s';`, IPPort)
	resp, err := mgr.QueryLeader(ctx, sql, cluster)
	if err != nil {
		return nil, err
	}

	resMap, err := postgres.ParseQuery(string(resp))
	if err != nil {
		return nil, err
	}

	if resMap[0]["connected"] != nil {
		res.Connected = cast.ToBool(resMap[0]["connected"])
	}
	if resMap[0]["log_delay_num"] != nil {
		res.LogDelayNum = cast.ToInt64(resMap[0]["log_delay_num"])
	}

	return res, nil
}

func (mgr *Manager) JoinCurrentMemberToCluster(ctx context.Context, cluster *dcs.Cluster) error {
	// use the env KB_POD_FQDN consistently with the startup script
	sql := fmt.Sprintf(`alter system consensus add follower '%s:%d';`,
		viper.GetString("KB_POD_FQDN"), mgr.Config.GetDBPort())

	_, err := mgr.ExecLeader(ctx, sql, cluster)
	if err != nil {
		mgr.Logger.Error(err, fmt.Sprintf("exec sql:%s failed", sql))
		return err
	}

	return nil
}

func (mgr *Manager) LeaveMemberFromCluster(ctx context.Context, _ *dcs.Cluster, host string) error {
	sql := fmt.Sprintf(`alter system consensus drop follower '%s:%d';`,
		host, mgr.Config.GetDBPort())

	// only leader can delete member, so don't need to get pool
	_, err := mgr.ExecWithHost(ctx, sql, "")
	if err != nil {
		mgr.Logger.Error(err, fmt.Sprintf("exec sql:%s failed", sql))
		return err
	}

	return nil
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
