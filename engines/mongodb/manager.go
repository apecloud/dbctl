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
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
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

func (mgr *Manager) Lock(ctx context.Context, reason string) error {
	mgr.Logger.Info(fmt.Sprintf("Lock db: %s", reason))
	m := bson.D{
		{Key: "fsync", Value: 1},
		{Key: "lock", Value: true},
		{Key: "comment", Value: reason},
	}
	lockResp := LockResp{}

	response := mgr.Client.Database("admin").RunCommand(ctx, m)
	if response.Err() != nil {
		mgr.Logger.Info(fmt.Sprintf("Lock db (%s) failed", reason), "error", response.Err().Error())
		return response.Err()
	}
	if err := response.Decode(&lockResp); err != nil {
		err := errors.Wrap(err, "failed to decode lock response")
		return err
	}

	if lockResp.OK != 1 {
		err := errors.Errorf("mongo says: %s", lockResp.Errmsg)
		return err
	}
	mgr.IsLocked = true
	mgr.Logger.Info(fmt.Sprintf("Lock db success times: %d", lockResp.LockCount))
	return nil
}

func (mgr *Manager) Unlock(ctx context.Context) error {
	mgr.Logger.Info("Unlock db")
	m := bson.M{"fsyncUnlock": 1}
	unlockResp := LockResp{}
	response := mgr.Client.Database("admin").RunCommand(ctx, m)
	if response.Err() != nil {
		mgr.Logger.Info("Unlock db failed", "error", response.Err().Error())
		return response.Err()
	}
	if err := response.Decode(&unlockResp); err != nil {
		err := errors.Wrap(err, "failed to decode unlock response")
		return err
	}

	if unlockResp.OK != 1 {
		err := errors.Errorf("mongo says: %s", unlockResp.Errmsg)
		return err
	}
	for unlockResp.LockCount > 0 {
		response = mgr.Client.Database("admin").RunCommand(ctx, m)
		if response.Err() != nil {
			mgr.Logger.Info("Unlock db failed", "error", response.Err().Error())
			return response.Err()
		}
		if err := response.Decode(&unlockResp); err != nil {
			err := errors.Wrap(err, "failed to decode unlock response")
			return err
		}
	}
	mgr.IsLocked = false
	mgr.Logger.Info("Unlock db success")
	return nil
}
