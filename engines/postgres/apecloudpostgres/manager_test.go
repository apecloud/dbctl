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
	"testing"

	"github.com/pashagolub/pgxmock/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/apecloud/dbctl/constant"
	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/models"
	"github.com/apecloud/dbctl/engines/postgres"
)

func MockDatabase(t *testing.T) (*Manager, pgxmock.PgxPoolIface, error) {
	properties := map[string]string{
		postgres.ConnectionURLKey: "user=test password=test host=localhost port=5432 dbname=postgres",
	}
	testConfig, err := postgres.NewConfig(properties)
	assert.NotNil(t, testConfig)
	assert.Nil(t, err)

	viper.Set(constant.KBEnvPodName, "test-pod-0")
	viper.Set(constant.KBEnvClusterCompName, "test")
	viper.Set(constant.KBEnvNamespace, "default")
	viper.Set(postgres.PGDATA, "test")
	mock, err := pgxmock.NewPool(pgxmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}

	dbManager, err := NewManager(engines.Properties(properties))
	if err != nil {
		t.Fatal(err)
	}

	manager := dbManager.(*Manager)
	manager.Pool = mock

	return manager, mock, err
}

func TestIsConsensusReadyUp(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("consensus has been ready up", func(t *testing.T) {
		mock.ExpectQuery("SELECT extname FROM pg_extension").
			WillReturnRows(pgxmock.NewRows([]string{"extname"}).AddRow("consensus_monitor"))

		isReadyUp := manager.isConsensusReadyUp(ctx)
		assert.True(t, isReadyUp)
	})

	t.Run("consensus has not been ready up", func(t *testing.T) {
		mock.ExpectQuery("SELECT extname FROM pg_extension").
			WillReturnRows(pgxmock.NewRows([]string{"extname"}))

		isReadyUp := manager.isConsensusReadyUp(ctx)
		assert.False(t, isReadyUp)
	})

	t.Run("query pg_extension error", func(t *testing.T) {
		mock.ExpectQuery("SELECT extname FROM pg_extension").
			WillReturnError(fmt.Errorf("some errors"))

		isReadyUp := manager.isConsensusReadyUp(ctx)
		assert.False(t, isReadyUp)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestIsDBStartupReady(t *testing.T) {
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("db start up has been set", func(t *testing.T) {
		manager.DBStartupReady = true

		isReady := manager.IsDBStartupReady()
		assert.True(t, isReady)
	})

	t.Run("ping db failed", func(t *testing.T) {
		manager.DBStartupReady = false
		mock.ExpectPing().
			WillReturnError(fmt.Errorf("some error"))

		isReady := manager.IsDBStartupReady()
		assert.False(t, isReady)
	})

	t.Run("ping db success but consensus not ready up", func(t *testing.T) {
		manager.DBStartupReady = false
		mock.ExpectPing()
		mock.ExpectQuery("SELECT extname FROM pg_extension").
			WillReturnRows(pgxmock.NewRows([]string{"extname"}))

		isReady := manager.IsDBStartupReady()
		assert.False(t, isReady)
	})

	t.Run("db is startup ready", func(t *testing.T) {
		manager.DBStartupReady = false
		mock.ExpectPing()
		mock.ExpectQuery("SELECT extname FROM pg_extension").
			WillReturnRows(pgxmock.NewRows([]string{"extname"}).AddRow("consensus_monitor"))

		isReady := manager.IsDBStartupReady()
		assert.True(t, isReady)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestGetMemberRoleWithHost(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()
	roles := []string{models.FOLLOWER, models.CANDIDATE, models.LEADER, models.LEARNER, ""}

	t.Run("query paxos role failed", func(t *testing.T) {
		mock.ExpectQuery("select role from consensus_member_status;").
			WillReturnError(fmt.Errorf("some error"))

		role, err := manager.GetMemberRoleWithHost(ctx, "")
		assert.Equal(t, "", role)
		assert.NotNil(t, err)
	})

	t.Run("parse query failed", func(t *testing.T) {
		mock.ExpectQuery("select role from consensus_member_status;").
			WillReturnRows(pgxmock.NewRows([]string{"role"}))

		role, err := manager.GetMemberRoleWithHost(ctx, "")
		assert.Equal(t, "", role)
		assert.NotNil(t, err)
	})

	t.Run("get member role with host success", func(t *testing.T) {
		for _, r := range roles {
			mock.ExpectQuery("select role from consensus_member_status;").
				WillReturnRows(pgxmock.NewRows([]string{"role"}).AddRow(r))

			role, err := manager.GetMemberRoleWithHost(ctx, "")
			assert.Equal(t, strings.ToLower(r), role)
			assert.Nil(t, err)
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestIsLeaderWithHost(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("get member role with host failed", func(t *testing.T) {
		mock.ExpectQuery("select role from consensus_member_status;").
			WillReturnError(fmt.Errorf("some error"))

		isLeader, err := manager.IsLeaderWithHost(ctx, "")
		assert.False(t, isLeader)
		assert.NotNil(t, err)
	})

	t.Run("check is leader success", func(t *testing.T) {
		mock.ExpectQuery("select role from consensus_member_status;").
			WillReturnRows(pgxmock.NewRows([]string{"role"}).AddRow("Leader"))

		isLeader, err := manager.IsLeaderWithHost(ctx, "")
		assert.True(t, isLeader)
		assert.Nil(t, err)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
