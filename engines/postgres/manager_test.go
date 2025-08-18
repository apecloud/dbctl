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

func TestReadWrite(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("write check success", func(t *testing.T) {
		mock.ExpectExec(`create table if not exists`).
			WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))

		ok := manager.WriteCheck(ctx, "")
		assert.True(t, ok)
	})

	t.Run("write check failed", func(t *testing.T) {
		mock.ExpectExec(`create table if not exists`).
			WillReturnError(fmt.Errorf("some error"))

		ok := manager.WriteCheck(ctx, "")
		assert.False(t, ok)
	})

	t.Run("read check success", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnRows(pgxmock.NewRows([]string{"check_ts"}).AddRow(1))

		ok := manager.ReadCheck(ctx, "")
		assert.True(t, ok)
	})

	t.Run("read check failed", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnError(fmt.Errorf("some error"))

		ok := manager.ReadCheck(ctx, "")
		assert.False(t, ok)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
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

func TestPgReload(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("pg reload success", func(t *testing.T) {
		mock.ExpectExec("select pg_reload_conf()").
			WillReturnResult(pgxmock.NewResult("select", 1))

		err := manager.PgReload(ctx)
		assert.Nil(t, err)
	})

	t.Run("pg reload failed", func(t *testing.T) {
		mock.ExpectExec("select pg_reload_conf()").
			WillReturnError(fmt.Errorf("some error"))

		err := manager.PgReload(ctx)
		assert.NotNil(t, err)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestLock(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("alter system failed", func(t *testing.T) {
		mock.ExpectExec("alter system").
			WillReturnError(fmt.Errorf("alter system failed"))

		err := manager.Lock(ctx, "test")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "alter system failed")
	})

	t.Run("pg reload failed", func(t *testing.T) {
		mock.ExpectExec("alter system").
			WillReturnResult(pgxmock.NewResult("alter", 1))
		mock.ExpectExec("select pg_reload_conf()").
			WillReturnError(fmt.Errorf("pg reload failed"))
		err := manager.Lock(ctx, "test")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "pg reload failed")
	})

	t.Run("lock success", func(t *testing.T) {
		mock.ExpectExec("alter system").
			WillReturnResult(pgxmock.NewResult("alter", 1))
		mock.ExpectExec("select pg_reload_conf()").
			WillReturnResult(pgxmock.NewResult("select", 1))
		err := manager.Lock(ctx, "test")
		assert.Nil(t, err)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestUnlock(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := MockDatabase(t)
	defer mock.Close()

	t.Run("alter system failed", func(t *testing.T) {
		mock.ExpectExec("alter system").
			WillReturnError(fmt.Errorf("alter system failed"))

		err := manager.Unlock(ctx)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "alter system failed")
	})

	t.Run("pg reload failed", func(t *testing.T) {
		mock.ExpectExec("alter system").
			WillReturnResult(pgxmock.NewResult("alter", 1))
		mock.ExpectExec("select pg_reload_conf()").
			WillReturnError(fmt.Errorf("pg reload failed"))
		err := manager.Unlock(ctx)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "pg reload failed")
	})

	t.Run("unlock success", func(t *testing.T) {
		mock.ExpectExec("alter system").
			WillReturnResult(pgxmock.NewResult("alter", 1))
		mock.ExpectExec("select pg_reload_conf()").
			WillReturnResult(pgxmock.NewResult("select", 1))
		err := manager.Unlock(ctx)
		assert.Nil(t, err)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
