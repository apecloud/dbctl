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
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/apecloud/dbctl/constant"
)

const (
	fakePodName         = "fake-mysql-0"
	fakeClusterCompName = "test-mysql"
)

func TestNewManager(t *testing.T) {
	defer viper.Reset()

	viper.SetDefault(constant.KBEnvPodName, "pod-test-0")
	viper.SetDefault(constant.KBEnvClusterCompName, "cluster-component-test")
	viper.SetDefault(constant.KBEnvNamespace, "namespace-test")
	t.Run("new default manager", func(t *testing.T) {
		managerIFace, err := NewManager()
		assert.NotNil(t, managerIFace)
		assert.Nil(t, err)

		manager, ok := managerIFace.(*Manager)
		assert.True(t, ok)
		assert.Equal(t, "pod-test-0", manager.CurrentMemberName)
		assert.Equal(t, "cluster-component-test", manager.ClusterCompName)
		assert.Equal(t, uint(1), manager.serverID)
	})

	viper.Set(constant.KBEnvPodName, "fake")
	viper.Set(constant.KBEnvClusterCompName, fakeClusterCompName)
	t.Run("get server id failed", func(t *testing.T) {
		manager, err := NewManager()

		assert.Nil(t, manager)
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "the format of member name is wrong")
	})
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
