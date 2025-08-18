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

package wesql

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/constant"
	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/mysql"
)

const (
	fakePodName         = "test-wesql-0"
	fakeClusterCompName = "test-wesql"
	fakeNamespace       = "fake-namespace"
)

func mockDatabase(t *testing.T) (*Manager, sqlmock.Sqlmock, error) {
	manager := &Manager{
		mysql.Manager{
			DBManagerBase: engines.DBManagerBase{
				CurrentMemberName: fakePodName,
				ClusterCompName:   fakeClusterCompName,
				Namespace:         fakeNamespace,
				Logger:            ctrl.Log.WithName("WeSQL-TEST"),
			},
		},
	}

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	manager.DB = db

	return manager, mock, err
}

func TestNewManager(t *testing.T) {
	t.Run("new config failed", func(t *testing.T) {
		manager, err := NewManager(fakePropertiesWithWrongPem)

		assert.Nil(t, manager)
		assert.NotNil(t, err)
	})

	t.Run("new mysql manager failed", func(t *testing.T) {
		manager, err := NewManager(fakeProperties)

		assert.Nil(t, manager)
		assert.NotNil(t, err)
	})

	viper.Set(constant.KBEnvPodName, fakePodName)
	defer viper.Reset()
	t.Run("new manger successfully", func(t *testing.T) {
		manager, err := NewManager(fakeProperties)

		assert.Nil(t, err)
		assert.NotNil(t, manager)
	})
}
