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
	"testing"

	"github.com/pashagolub/pgxmock/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/apecloud/dbctl/constant"
	"github.com/apecloud/dbctl/dcs"
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
	viper.Set(postgres.PGMAJOR, 14)
	mock, err := pgxmock.NewPool(pgxmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}

	dbManager, err := NewManager(properties)
	if err != nil {
		t.Fatal(err)
	}
	manager := dbManager.(*Manager)
	manager.Pool = mock

	return manager, mock, err
}

func TestIsLeader(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("get member role primary", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnRows(pgxmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(false))

		isLeader, err := manager.IsLeader(ctx, nil)
		assert.Nil(t, err)
		assert.Equal(t, true, isLeader)
	})

	t.Run("get member role secondary", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnRows(pgxmock.NewRows([]string{"pg_is_in_recovery"}).AddRow(true))

		isLeader, err := manager.IsLeader(ctx, nil)
		assert.Nil(t, err)
		assert.Equal(t, false, isLeader)
	})

	t.Run("query failed", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnError(fmt.Errorf("some error"))

		isLeader, err := manager.IsLeader(ctx, nil)
		assert.NotNil(t, err)
		assert.Equal(t, false, isLeader)
	})

	t.Run("parse query failed", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnRows(pgxmock.NewRows([]string{"pg_is_in_recovery"}))
		isLeader, err := manager.IsLeader(ctx, nil)
		assert.NotNil(t, err)
		assert.Equal(t, false, isLeader)
	})

	t.Run("has set isLeader", func(t *testing.T) {
		manager.SetIsLeader(true)
		isLeader, err := manager.IsLeader(ctx, nil)
		assert.Nil(t, err)
		assert.Equal(t, true, isLeader)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestHasOtherHealthyMembers(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()
	cluster := &dcs.Cluster{}
	cluster.Members = append(cluster.Members, dcs.Member{
		Name: manager.CurrentMemberName,
	})

	t.Run("", func(t *testing.T) {
		members := manager.HasOtherHealthyMembers(ctx, cluster, manager.CurrentMemberName)
		assert.Equal(t, 0, len(members))
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
