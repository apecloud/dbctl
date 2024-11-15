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
	"github.com/spf13/viper"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/constant"
	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/etcd"
	"github.com/apecloud/dbctl/engines/foxlake"
	"github.com/apecloud/dbctl/engines/models"
	"github.com/apecloud/dbctl/engines/mongodb"
	"github.com/apecloud/dbctl/engines/mysql"
	"github.com/apecloud/dbctl/engines/nebula"
	"github.com/apecloud/dbctl/engines/oceanbase"
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

type managerNewFunc func(engines.Properties) (engines.DBManager, error)

var managerNewFuncs = make(map[string]managerNewFunc)

// Lorry runs with a single database engine instance at a time,
// so only one dbManager is initialized and cached here during execution.
var dbManager engines.DBManager
var fs = afero.NewOsFs()

func init() {
	RegisterEngine(models.MySQL, "consensus", wesql.NewManager, mysql.NewCommands)
	RegisterEngine(models.MySQL, "replication", mysql.NewManager, mysql.NewCommands)
	RegisterEngine(models.Redis, "replication", redis.NewManager, redis.NewCommands)
	RegisterEngine(models.ETCD, "consensus", etcd.NewManager, nil)
	RegisterEngine(models.MongoDB, "consensus", mongodb.NewManager, mongodb.NewCommands)
	RegisterEngine(models.PolarDBX, "consensus", polardbx.NewManager, mysql.NewCommands)
	RegisterEngine(models.PostgreSQL, "replication", vanillapostgres.NewManager, postgres.NewCommands)
	RegisterEngine(models.PostgreSQL, "consensus", apecloudpostgres.NewManager, postgres.NewCommands)
	RegisterEngine(models.FoxLake, "", nil, foxlake.NewCommands)
	RegisterEngine(models.Nebula, "", nil, nebula.NewCommands)
	RegisterEngine(models.PulsarProxy, "", nil, pulsar.NewProxyCommands)
	RegisterEngine(models.PulsarBroker, "", nil, pulsar.NewBrokerCommands)
	RegisterEngine(models.Oceanbase, "", oceanbase.NewManager, oceanbase.NewCommands)
	RegisterEngine(models.Oracle, "", nil, oracle.NewCommands)
	RegisterEngine(models.OpenGauss, "", nil, opengauss.NewCommands)

	// support component definition without workloadType
	RegisterEngine(models.WeSQL, "", wesql.NewManager, mysql.NewCommands)
	RegisterEngine(models.MySQL, "", mysql.NewManager, mysql.NewCommands)
	RegisterEngine(models.Redis, "", redis.NewManager, redis.NewCommands)
	RegisterEngine(models.ETCD, "", etcd.NewManager, nil)
	RegisterEngine(models.MongoDB, "", mongodb.NewManager, mongodb.NewCommands)
	RegisterEngine(models.PolarDBX, "", polardbx.NewManager, mysql.NewCommands)
	RegisterEngine(models.PostgreSQL, "", vanillapostgres.NewManager, postgres.NewCommands)
	RegisterEngine(models.VanillaPostgreSQL, "", vanillapostgres.NewManager, postgres.NewCommands)
	RegisterEngine(models.ApecloudPostgreSQL, "", apecloudpostgres.NewManager, postgres.NewCommands)
}

func RegisterEngine(characterType models.EngineType, workloadType string, newFunc managerNewFunc, newCommand engines.NewCommandFunc) {
	key := strings.ToLower(string(characterType) + "_" + workloadType)
	managerNewFuncs[key] = newFunc
	engines.NewCommandFuncs[string(characterType)] = newCommand
}

func GetManagerNewFunc(characterType, workloadType string) managerNewFunc {
	key := strings.ToLower(characterType + "_" + workloadType)
	return managerNewFuncs[key]
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

func InitDBManager(configDir string) error {
	if dbManager != nil {
		return nil
	}

	ctrl.Log.Info("Initialize DB manager")
	workloadType := viper.GetString(constant.KBEnvWorkloadType)
	if workloadType == "" {
		ctrl.Log.Info(constant.KBEnvWorkloadType + " ENV not set")
	}

	engineType := viper.GetString(constant.KBEnvEngineType)
	if viper.IsSet(constant.KBEnvBuiltinHandler) && engineType == "" {
		workloadType = ""
		engineType = viper.GetString(constant.KBEnvBuiltinHandler)
	}
	if engineType == "" {
		return errors.New("engine typpe not set")
	}

	err := GetAllComponent(configDir) // find all builtin config file and read
	if err != nil {                   // Handle errors reading the config file
		return errors.Wrap(err, "fatal error config file")
	}

	properties := GetProperties(engineType)
	newFunc := GetManagerNewFunc(engineType, workloadType)
	if newFunc == nil {
		return errors.Errorf("no db manager for characterType %s and workloadType %s", engineType, workloadType)
	}
	mgr, err := newFunc(properties)
	if err != nil {
		return err
	}

	dbManager = mgr
	return nil
}

type Component struct {
	Name string
	Spec ComponentSpec
}

type ComponentSpec struct {
	Version  string
	Metadata []kv
}

type kv struct {
	Name  string
	Value string
}

var Name2Property = map[string]engines.Properties{}

func readConfig(filename string) (string, engines.Properties, error) {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(filename)
	if err := viper.ReadInConfig(); err != nil {
		return "", nil, err
	}
	component := &Component{}
	if err := viper.Unmarshal(component); err != nil {
		return "", nil, err
	}
	properties := make(engines.Properties)
	properties["version"] = component.Spec.Version
	for _, pair := range component.Spec.Metadata {
		properties[pair.Name] = pair.Value
	}
	return component.Name, properties, nil
}

func GetAllComponent(dir string) error {
	files, err := afero.ReadDir(fs, dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		name, properties, err := readConfig(dir + "/" + file.Name())
		if err != nil {
			return err
		}
		Name2Property[name] = properties
	}
	return nil
}

func GetProperties(name string) engines.Properties {
	return Name2Property[name]
}
