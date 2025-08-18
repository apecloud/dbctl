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

	"github.com/apecloud/dbctl/engines/models"
)

type DBManager interface {
	IsRunning() bool

	IsDBStartupReady() bool

	IsFirstMember() bool
	GetReplicaRole(context.Context) (string, error)

	// IsPromoted is applicable only to consensus cluster, which is used to
	// check if DB has complete switchover.
	// for replicationset cluster,  it will always be true.
	IsPromoted(context.Context) bool

	Stop() error

	// GetHealthiestMember(*dcs.Cluster, string) *dcs.Member
	// IsHealthiestMember(*dcs.Cluster) bool

	GetCurrentMemberName() string

	// Functions related to account manage
	IsRootCreated(context.Context) (bool, error)
	CreateRoot(context.Context) error

	// Readonly lock for disk full
	Lock(context.Context, string) error
	Unlock(context.Context) error

	// sql query
	Exec(context.Context, string) (int64, error)
	Query(context.Context, string) ([]byte, error)

	// user management
	ListUsers(context.Context) ([]models.UserInfo, error)
	ListSystemAccounts(context.Context) ([]models.UserInfo, error)
	CreateUser(context.Context, string, string) error
	DeleteUser(context.Context, string) error
	DescribeUser(context.Context, string) (*models.UserInfo, error)
	GrantUserRole(context.Context, string, string) error
	RevokeUserRole(context.Context, string, string) error

	GetPort() (int, error)

	GetLogger() logr.Logger

	ShutDownWithWait()
}
