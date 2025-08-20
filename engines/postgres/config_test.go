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
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetPostgresqlMetadata(t *testing.T) {
	t.Run("With defaults", func(t *testing.T) {
		metadata, err := NewConfig()
		assert.Nil(t, err)
		assert.Equal(t, "postgres", metadata.username)
		assert.Equal(t, "docker", metadata.password)
		assert.Equal(t, "localhost", metadata.host)
		assert.Equal(t, 5432, metadata.port)
		assert.Equal(t, "postgres", metadata.database)
		assert.Equal(t, int32(1), metadata.minConnections)
		assert.Equal(t, int32(10), metadata.maxConnections)
	})

	t.Run("set env", func(t *testing.T) {
		viper.Set(EnvRootUser, "test")
		viper.Set(EnvRootPassword, "test_pwd")
		metadata, err := NewConfig()
		assert.Nil(t, err)

		assert.Equal(t, "test", metadata.username)
		assert.Equal(t, "test_pwd", metadata.password)
	})
}
