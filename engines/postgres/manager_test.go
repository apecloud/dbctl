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
	"fmt"
	"testing"

	"github.com/pashagolub/pgxmock/v2"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/apecloud/dbctl/constant"
)

func MockDatabase(t *testing.T) (*Manager, pgxmock.PgxPoolIface, error) {
	testConfig, err := NewConfig()
	assert.NotNil(t, testConfig)
	assert.Nil(t, err)

	viper.Set(constant.KBEnvPodName, "test-pod-0")
	viper.Set(constant.KBEnvClusterCompName, "test")
	viper.Set(constant.KBEnvNamespace, "default")
	viper.Set(PGDATA, "test")
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

func TestPgIsReady(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("pg is ready", func(t *testing.T) {
		mock.ExpectPing()

		if isReady := manager.IsPgReady(ctx); !isReady {
			t.Errorf("test pg is ready failed")
		}
	})

	t.Run("pg is not ready", func(t *testing.T) {
		mock.ExpectPing().WillReturnError(fmt.Errorf("can't ping to db"))
		if isReady := manager.IsPgReady(ctx); isReady {
			t.Errorf("expect pg is not ready, but get ready")
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
