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

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/apecloud/dbctl/constant"
)

const (
	fakePodName         = "fake-mysql-0"
	fakeClusterCompName = "test-mysql"
	fakeNamespace       = "fake-namespace"
	fakeDBPort          = "fake-port"
)

func TestNewManager(t *testing.T) {
	defer viper.Reset()

	t.Run("new config failed", func(t *testing.T) {
		manager, err := NewManager(fakePropertiesWithPem)

		assert.Nil(t, manager)
		assert.NotNil(t, err)
	})

	viper.Set(constant.KBEnvPodName, "fake")
	viper.Set(constant.KBEnvClusterCompName, fakeClusterCompName)
	viper.Set(constant.KBEnvNamespace, fakeNamespace)
	t.Run("get server id failed", func(t *testing.T) {
		manager, err := NewManager(fakeProperties)

		assert.Nil(t, manager)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "the format of member name is wrong")
	})

	viper.Set(constant.KBEnvPodName, fakePodName)
	t.Run("get local connection failed", func(t *testing.T) {
		manager, err := NewManager(fakePropertiesWithWrongURL)

		assert.Nil(t, manager)
		assert.NotNil(t, err)
	})

	t.Run("new manager successfully", func(t *testing.T) {
		managerIFace, err := NewManager(fakeProperties)
		assert.Nil(t, err)

		manager, ok := managerIFace.(*Manager)
		assert.True(t, ok)
		assert.Equal(t, fakePodName, manager.CurrentMemberName)
		assert.Equal(t, fakeNamespace, manager.Namespace)
		assert.Equal(t, fakeClusterCompName, manager.ClusterCompName)
		assert.Equal(t, uint(1), manager.serverID)
	})
}

func TestManager_IsRunning(t *testing.T) {
	manager, mock, _ := mockDatabase(t)

	t.Run("Too many connections", func(t *testing.T) {
		mock.ExpectPing().
			WillReturnError(&mysql.MySQLError{Number: 1040})

		isRunning := manager.IsRunning()
		assert.True(t, isRunning)
	})

	t.Run("DB is not ready", func(t *testing.T) {
		mock.ExpectPing().
			WillReturnError(fmt.Errorf("some error"))

		isRunning := manager.IsRunning()
		assert.False(t, isRunning)
	})

	t.Run("ping db overtime", func(t *testing.T) {
		mock.ExpectPing().WillDelayFor(time.Second)

		isRunning := manager.IsRunning()
		assert.False(t, isRunning)
	})

	t.Run("db is running", func(t *testing.T) {
		mock.ExpectPing()

		isRunning := manager.IsRunning()
		assert.True(t, isRunning)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_IsDBStartupReady(t *testing.T) {
	manager, mock, _ := mockDatabase(t)

	t.Run("db has start up", func(t *testing.T) {
		manager.DBStartupReady = true
		defer func() {
			manager.DBStartupReady = false
		}()

		dbReady := manager.IsDBStartupReady()
		assert.True(t, dbReady)
	})

	t.Run("ping db failed", func(t *testing.T) {
		mock.ExpectPing().WillDelayFor(time.Second)

		dbReady := manager.IsDBStartupReady()
		assert.False(t, dbReady)
	})

	t.Run("check db start up successfully", func(t *testing.T) {
		mock.ExpectPing()

		dbReady := manager.IsDBStartupReady()
		assert.True(t, dbReady)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_WriteCheck(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)

	t.Run("write check failed", func(t *testing.T) {
		mock.ExpectExec("CREATE DATABASE IF NOT EXISTS kubeblocks;").
			WillReturnError(fmt.Errorf("some error"))

		canWrite := manager.WriteCheck(ctx, manager.DB)
		assert.NotNil(t, canWrite)
	})

	t.Run("write check successfully", func(t *testing.T) {
		mock.ExpectExec("CREATE DATABASE IF NOT EXISTS kubeblocks;").
			WillReturnResult(sqlmock.NewResult(1, 1))

		canWrite := manager.WriteCheck(ctx, manager.DB)
		assert.Nil(t, canWrite)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_ReadCheck(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)

	t.Run("no rows in result set", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnError(sql.ErrNoRows)

		canRead := manager.ReadCheck(ctx, manager.DB)
		assert.Nil(t, canRead)
	})

	t.Run("no healthy database", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnError(&mysql.MySQLError{Number: 1049})

		canRead := manager.ReadCheck(ctx, manager.DB)
		assert.Nil(t, canRead)
	})

	t.Run("Read check failed", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnError(fmt.Errorf("some error"))

		canRead := manager.ReadCheck(ctx, manager.DB)
		assert.NotNil(t, canRead)
	})

	t.Run("Read check successfully", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnRows(sqlmock.NewRows([]string{"check_ts"}).AddRow(1))

		canRead := manager.ReadCheck(ctx, manager.DB)
		assert.Nil(t, canRead)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_GetGlobalState(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)

	t.Run("get global state successfully", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnRows(sqlmock.NewRows([]string{"@@global.hostname", "@@global.server_uuid", "@@global.gtid_executed", "@@global.gtid_purged", "@@global.read_only", "@@global.super_read_only"}).
				AddRow(fakePodName, fakeServerUUID, fakeGTIDString, fakeGTIDSet, 1, 1))

		globalState, err := manager.GetGlobalState(ctx, manager.DB)
		assert.Nil(t, err)
		assert.NotNil(t, globalState)
		assert.Equal(t, fakePodName, globalState["hostname"])
		assert.Equal(t, fakeServerUUID, globalState["server_uuid"])
		assert.Equal(t, fakeGTIDString, globalState["gtid_executed"])
		assert.Equal(t, fakeGTIDSet, globalState["gtid_purged"])
		assert.Equal(t, "1", globalState["read_only"])
		assert.Equal(t, "1", globalState["super_read_only"])
	})

	t.Run("get global state failed", func(t *testing.T) {
		mock.ExpectQuery("select").
			WillReturnError(fmt.Errorf("some error"))

		globalState, err := manager.GetGlobalState(ctx, manager.DB)
		assert.Nil(t, globalState)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "some error")
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_GetSlaveStatus(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)

	t.Run("query rows map failed", func(t *testing.T) {
		mock.ExpectQuery("show slave status").
			WillReturnError(fmt.Errorf("some error"))

		slaveStatus, err := manager.GetSlaveStatus(ctx, manager.DB)
		assert.Nil(t, slaveStatus)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "some error")
	})

	t.Run("get slave status successfully", func(t *testing.T) {
		mock.ExpectQuery("show slave status").
			WillReturnRows(sqlmock.NewRows([]string{"Seconds_Behind_Master", "Slave_IO_Running"}).AddRow("249904", "Yes"))

		slaveStatus, err := manager.GetSlaveStatus(ctx, manager.DB)
		assert.Nil(t, err)
		assert.Equal(t, "249904", slaveStatus.GetString("Seconds_Behind_Master"))
		assert.Equal(t, "Yes", slaveStatus.GetString("Slave_IO_Running"))
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_GetMasterStatus(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)

	t.Run("query rows map failed", func(t *testing.T) {
		mock.ExpectQuery("show master status").
			WillReturnError(fmt.Errorf("some error"))

		slaveStatus, err := manager.GetMasterStatus(ctx, manager.DB)
		assert.Nil(t, slaveStatus)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "some error")
	})

	t.Run("get slave status successfully", func(t *testing.T) {
		mock.ExpectQuery("show master status").
			WillReturnRows(sqlmock.NewRows([]string{"File", "Executed_Gtid_Set"}).AddRow("master-bin.000002", fakeGTIDSet))

		slaveStatus, err := manager.GetMasterStatus(ctx, manager.DB)
		assert.Nil(t, err)
		assert.Equal(t, "master-bin.000002", slaveStatus.GetString("File"))
		assert.Equal(t, fakeGTIDSet, slaveStatus.GetString("Executed_Gtid_Set"))
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_Demote(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)

	t.Run("execute promote failed", func(t *testing.T) {
		mock.ExpectExec("set global read_only=on").
			WillReturnError(fmt.Errorf("some error"))

		err := manager.Demote(ctx)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "some error")
	})

	t.Run("execute promote successfully", func(t *testing.T) {
		mock.ExpectExec("set global read_only=on").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err := manager.Demote(ctx)
		assert.Nil(t, err)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_isRecoveryConfOutdated(t *testing.T) {
	manager, _, _ := mockDatabase(t)
	manager.slaveStatus = RowMap{}

	t.Run("slaveStatus empty", func(t *testing.T) {
		outdated := manager.isRecoveryConfOutdated(fakePodName)
		assert.True(t, outdated)
	})

	t.Run("slave status error", func(t *testing.T) {
		manager.slaveStatus = RowMap{
			"Last_IO_Error": CellData{String: "some error"},
		}

		outdated := manager.isRecoveryConfOutdated(fakePodName)
		assert.True(t, outdated)
	})
}

func TestManager_Lock(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)

	t.Run("lock failed", func(t *testing.T) {
		mock.ExpectExec("set global read_only=on").
			WillReturnError(fmt.Errorf("some error"))

		err := manager.Lock(ctx, "")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "some error")
		assert.False(t, manager.IsLocked)
	})

	t.Run("lock successfully", func(t *testing.T) {
		mock.ExpectExec("set global read_only=on").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err := manager.Lock(ctx, "")
		assert.Nil(t, err)
		assert.True(t, manager.IsLocked)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}

func TestManager_Unlock(t *testing.T) {
	ctx := context.TODO()
	manager, mock, _ := mockDatabase(t)
	manager.IsLocked = true

	t.Run("unlock failed", func(t *testing.T) {
		mock.ExpectExec("set global read_only=off").
			WillReturnError(fmt.Errorf("some error"))

		err := manager.Unlock(ctx)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "some error")
		assert.True(t, manager.IsLocked)
	})

	t.Run("lock successfully", func(t *testing.T) {
		mock.ExpectExec("set global read_only=off").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err := manager.Unlock(ctx)
		assert.Nil(t, err)
		assert.False(t, manager.IsLocked)
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %v", err)
	}
}
