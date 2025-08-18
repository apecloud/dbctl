/*
Copyright (C) 2022-2024 ApeCloud Co., Ltd

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

package register

import (
	"fmt"
	"testing"

	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/apecloud/dbctl/engines"
)

const (
	fakeEngine = "fake-db"
)

func TestInitDBManager(t *testing.T) {
	fs = afero.NewMemMapFs()
	viper.SetFs(fs)
	realDBManager := dbManager
	defer func() {
		fs = afero.NewOsFs()
		viper.Reset()
		dbManager = realDBManager
	}()

	t.Run("characterType not set", func(t *testing.T) {
		err := InitDBManager("")

		assert.NotNil(t, err)
		// assert.ErrorContains(t, err, "KB_SERVICE_CHARACTER_TYPE not set")
		_, err = GetDBManager()
		assert.NotNil(t, err)
		// assert.ErrorContains(t, err, "no db manager")
	})

	t.Run("new func nil", func(t *testing.T) {
		err := InitDBManager(fakeEngine)

		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "no db manager for engine fake-db")
		_, err = GetDBManager()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "no db manager")
	})

	fakeNewFunc := func() (engines.DBManager, error) {
		return nil, fmt.Errorf("some error")
	}
	EngineRegister(fakeEngine, fakeNewFunc, nil)
	t.Run("new func failed", func(t *testing.T) {
		err := InitDBManager(fakeEngine)

		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "some error")
		_, err = GetDBManager()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "no db manager")
	})

	fakeNewFunc = func() (engines.DBManager, error) {
		return &engines.MockManager{}, nil
	}
	EngineRegister(fakeEngine, fakeNewFunc, func() engines.ClusterCommands {
		return nil
	})
	t.Run("new func successfully", func(t *testing.T) {
		err := InitDBManager(fakeEngine)

		assert.Nil(t, err)
		_, err = GetDBManager()
		assert.Nil(t, err)
	})

	SetDBManager(&engines.MockManager{})
	t.Run("db manager exists", func(t *testing.T) {
		err := InitDBManager(fakeEngine)
		assert.Nil(t, err)
		_, err = GetDBManager()
		assert.Nil(t, err)
	})

	t.Run("new cluster command", func(t *testing.T) {
		_, err := NewClusterCommands("")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "unsupported engine type: ")
		_, err = NewClusterCommands(fakeEngine)
		assert.Nil(t, err)
	})
}
