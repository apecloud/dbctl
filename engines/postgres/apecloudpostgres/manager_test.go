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
	"github.com/apecloud/dbctl/engines/models"
	"github.com/apecloud/dbctl/engines/postgres"
)

func MockDatabase(t *testing.T) (*Manager, pgxmock.PgxPoolIface, error) {
	testConfig, err := postgres.NewConfig()
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

	dbManager, err := NewManager()
	if err != nil {
		t.Fatal(err)
	}

	manager := dbManager.(*Manager)
	manager.Pool = mock

	return manager, mock, err
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
