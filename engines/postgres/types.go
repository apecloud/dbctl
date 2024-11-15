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
	"github.com/apecloud/dbctl/dcs"
	"github.com/apecloud/dbctl/engines"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

var (
	ClusterHasNoLeader = errors.New("cluster has no leader now")
)

const (
	PGDATA  = "PGDATA"
	PGMAJOR = "PG_MAJOR"
)

type PgBaseIFace interface {
	GetMemberRoleWithHost(ctx context.Context, host string) (string, error)
	IsMemberHealthy(ctx context.Context, cluster *dcs.Cluster, member *dcs.Member) bool
	Query(ctx context.Context, sql string) (result []byte, err error)
	Exec(ctx context.Context, sql string) (result int64, err error)
}

type PgIFace interface {
	engines.DBManager
	PgBaseIFace
}

type PgxIFace interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
	Ping(ctx context.Context) error
}

// PgxPoolIFace is interface representing pgx pool
type PgxPoolIFace interface {
	PgxIFace
	Acquire(ctx context.Context) (*pgxpool.Conn, error)
	Close()
}

type ConsensusMemberHealthStatus struct {
	Connected   bool
	LogDelayNum int64
}
