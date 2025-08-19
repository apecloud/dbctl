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

package mongodb

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/engines"
)

type Manager struct {
	engines.DBManagerBase
	Client   *mongo.Client
	Database *mongo.Database
}

var Mgr *Manager
var _ engines.DBManager = &Manager{}

func NewManager() (engines.DBManager, error) {
	ctx := context.Background()
	logger := ctrl.Log.WithName("MongoDB")
	config, err := NewConfig()
	if err != nil {
		return nil, err
	}

	opts := options.Client().
		SetHosts(config.Hosts).
		SetReplicaSet(config.ReplSetName).
		SetAuth(options.Credential{
			Password: config.Password,
			Username: config.Username,
		}).
		SetWriteConcern(writeconcern.Majority()).
		SetReadPreference(readpref.Primary()).
		SetDirect(config.Direct)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(err, "connect to mongodb")
	}

	defer func() {
		if err != nil {
			derr := client.Disconnect(ctx)
			if derr != nil {
				logger.Info("failed to disconnect", "error", derr.Error())
			}
		}
	}()

	managerBase, err := engines.NewDBManagerBase(logger)
	if err != nil {
		return nil, err
	}

	Mgr = &Manager{
		DBManagerBase: *managerBase,
		Client:        client,
		Database:      client.Database(config.DatabaseName),
	}

	return Mgr, nil
}

func (mgr *Manager) IsDBStartupReady() bool {
	if mgr.DBStartupReady {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := mgr.Client.Ping(ctx, readpref.Primary())
	if err != nil {
		mgr.Logger.Info("DB is not ready", "error", err)
		return false
	}
	mgr.DBStartupReady = true
	mgr.Logger.Info("DB startup ready")
	return true
}

func (mgr *Manager) GetMemberState(ctx context.Context) (string, error) {
	status, err := mgr.GetReplSetStatus(ctx)
	if err != nil {
		mgr.Logger.Info("rs.status() error", "error", err.Error())
		return "", err
	}

	self := status.GetSelf()
	if self == nil {
		return "", nil
	}
	return strings.ToLower(self.StateStr), nil
}

func (mgr *Manager) GetReplSetStatus(ctx context.Context) (*ReplSetStatus, error) {
	return GetReplSetStatus(ctx, mgr.Client)
}

func (mgr *Manager) GetMemberAddrsFromRSConfig(rsConfig *RSConfig) []string {
	if rsConfig == nil {
		return []string{}
	}

	hosts := make([]string, len(rsConfig.Members))
	for i, member := range rsConfig.Members {
		hosts[i] = member.Host
	}
	return hosts
}
