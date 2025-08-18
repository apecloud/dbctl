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

func NewManager(properties engines.Properties) (engines.DBManager, error) {
	ctx := context.Background()
	logger := ctrl.Log.WithName("MongoDB")
	config, err := NewConfig(properties)
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

func (mgr *Manager) IsRootCreated(ctx context.Context) (bool, error) {
	if !mgr.IsFirstMember() {
		return true, nil
	}

	client, err := NewLocalUnauthClient(ctx)
	if err != nil {
		mgr.Logger.Info("Get local unauth client failed", "error", err)
		return false, err
	}
	defer client.Disconnect(ctx) //nolint:errcheck

	_, err = GetReplSetStatus(ctx, client)
	if err == nil {
		return false, nil
	}
	err = errors.Cause(err)
	if cmdErr, ok := err.(mongo.CommandError); ok && cmdErr.Name == "Unauthorized" {
		return true, nil
	}

	mgr.Logger.Info("Get replSet status with local unauth client failed", "error", err)

	_, err = mgr.GetReplSetStatus(ctx)
	if err == nil {
		return true, nil
	}

	mgr.Logger.Info("Get replSet status with local auth client failed", "error", err)
	return false, err

}

func (mgr *Manager) CreateRoot(ctx context.Context) error {
	if !mgr.IsFirstMember() {
		return nil
	}

	client, err := NewLocalUnauthClient(ctx)
	if err != nil {
		mgr.Logger.Info("Get local unauth client failed", "error", err)
		return err
	}
	defer client.Disconnect(ctx) //nolint:errcheck

	role := map[string]interface{}{
		"role": "root",
		"db":   "admin",
	}

	mgr.Logger.Info(fmt.Sprintf("Create user: %s, passwd: %s, roles: %v", config.Username, config.Password, role))
	err = CreateUser(ctx, client, config.Username, config.Password, role)
	if err != nil {
		mgr.Logger.Info("Create Root failed", "error", err)
		return err
	}

	return nil
}

func (mgr *Manager) IsRunning() bool {
	// ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	// defer cancel()

	// err := mgr.Client.Ping(ctx, readpref.Nearest())
	// if err != nil {
	// 	mgr.Logger.Infof("DB is not ready: %v", err)
	// 	return false
	// }
	return true
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

func (mgr *Manager) GetReplSetConfig(ctx context.Context) (*RSConfig, error) {
	return GetReplSetConfig(ctx, mgr.Client)
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

func (mgr *Manager) GetReplSetClientWithHosts(ctx context.Context, hosts []string) (*mongo.Client, error) {
	if len(hosts) == 0 {
		err := errors.New("Get replset client without hosts")
		mgr.Logger.Info("Get replset client without hosts", "error", err.Error())
		return nil, err
	}

	opts := options.Client().
		SetHosts(hosts).
		SetReplicaSet(config.ReplSetName).
		SetAuth(options.Credential{
			Password: config.Password,
			Username: config.Username,
		}).
		SetWriteConcern(writeconcern.Majority()).
		SetReadPreference(readpref.Primary()).
		SetDirect(false)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(err, "connect to mongodb")
	}
	return client, err
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
