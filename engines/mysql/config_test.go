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

	"github.com/spf13/afero"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	fs = afero.NewMemMapFs()
	defer func() {
		fs = afero.NewOsFs()
		viper.Reset()
	}()

	t.Run("with default", func(t *testing.T) {
		fakeConfig, err := NewConfig()
		assert.Nil(t, err)
		assert.NotNil(t, fakeConfig)
		assert.Equal(t, "root:@tcp(127.0.0.1:3306)/mysql?multiStatements=true", fakeConfig.URL)
		assert.Equal(t, 5, fakeConfig.MaxOpenConns)
		assert.Equal(t, 1, fakeConfig.MaxIdleConns)
	})
}

func TestConfig_GetLocalDBConn(t *testing.T) {
	t.Run("get DB connection with addr successfully", func(t *testing.T) {
		fakeConfig, err := NewConfig()
		assert.Nil(t, err)

		db, err := fakeConfig.GetLocalDBConn()
		assert.Nil(t, err)
		assert.NotNil(t, db)
	})
}
