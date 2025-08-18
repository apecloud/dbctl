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
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/etcd"
	"github.com/apecloud/dbctl/engines/foxlake"
	"github.com/apecloud/dbctl/engines/models"
	"github.com/apecloud/dbctl/engines/mongodb"
	"github.com/apecloud/dbctl/engines/mysql"
	"github.com/apecloud/dbctl/engines/nebula"
	"github.com/apecloud/dbctl/engines/opengauss"
	"github.com/apecloud/dbctl/engines/oracle"
	"github.com/apecloud/dbctl/engines/polardbx"
	"github.com/apecloud/dbctl/engines/postgres"
	"github.com/apecloud/dbctl/engines/postgres/apecloudpostgres"
	"github.com/apecloud/dbctl/engines/postgres/vanillapostgres"
	"github.com/apecloud/dbctl/engines/pulsar"
	"github.com/apecloud/dbctl/engines/redis"
	"github.com/apecloud/dbctl/engines/wesql"
)

type ManagerNewFunc func() (engines.DBManager, error)

var managerNewFunctions = make(map[string]ManagerNewFunc)

// Lorry runs with a single database engine instance at a time,
// so only one dbManager is initialized and cached here during execution.
var dbManager engines.DBManager
var fs = afero.NewOsFs()

func init() {
	EngineRegister(models.WeSQL, wesql.NewManager, mysql.NewCommands)
	EngineRegister(models.MySQL, mysql.NewManager, mysql.NewCommands)
	EngineRegister(models.Redis, redis.NewManager, redis.NewCommands)
	EngineRegister(models.ETCD, etcd.NewManager, nil)
	EngineRegister(models.MongoDB, mongodb.NewManager, mongodb.NewCommands)
	EngineRegister(models.PolarDBX, polardbx.NewManager, mysql.NewCommands)
	EngineRegister(models.PostgreSQL, vanillapostgres.NewManager, postgres.NewCommands)
	EngineRegister(models.VanillaPostgreSQL, vanillapostgres.NewManager, postgres.NewCommands)
	EngineRegister(models.ApecloudPostgreSQL, apecloudpostgres.NewManager, postgres.NewCommands)
	EngineRegister(models.FoxLake, nil, foxlake.NewCommands)
	EngineRegister(models.Nebula, nil, nebula.NewCommands)
	EngineRegister(models.PulsarProxy, nil, pulsar.NewProxyCommands)
	EngineRegister(models.PulsarBroker, nil, pulsar.NewBrokerCommands)
	EngineRegister(models.Oracle, nil, oracle.NewCommands)
	EngineRegister(models.OpenGauss, nil, opengauss.NewCommands)
}

func EngineRegister(characterType models.EngineType, newFunc ManagerNewFunc, newCommand engines.NewCommandFunc) {
	key := strings.ToLower(string(characterType))
	managerNewFunctions[key] = newFunc
	engines.NewCommandFuncs[string(characterType)] = newCommand
}

func GetManagerNewFunc(characterType string) ManagerNewFunc {
	key := strings.ToLower(characterType)
	return managerNewFunctions[key]
}

func SetDBManager(manager engines.DBManager) {
	dbManager = manager
}

func GetDBManager() (engines.DBManager, error) {
	if dbManager != nil {
		return dbManager, nil
	}

	return nil, errors.Errorf("no db manager")
}

func NewClusterCommands(typeName string) (engines.ClusterCommands, error) {
	newFunc, ok := engines.NewCommandFuncs[typeName]
	if !ok || newFunc == nil {
		return nil, fmt.Errorf("unsupported engine type: %s", typeName)
	}

	return newFunc(), nil
}

func InitDBManager(engineType string) error {
	if dbManager != nil {
		return nil
	}
	if engineType == "" {
		return errors.New("engine type not set")
	}

	ctrl.Log.Info("Initialize DB manager")
	newFunc := GetManagerNewFunc(engineType)
	if newFunc == nil {
		return errors.Errorf("no db manager for engine %s", engineType)
	}
	mgr, err := newFunc()
	if err != nil {
		return err
	}

	dbManager = mgr
	return nil
}
