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

package oceanbase

import (
	"strings"

	"github.com/spf13/viper"

	mysqlengine "github.com/apecloud/dbctl/engines/mysql"
)

type Config struct {
	*mysqlengine.Config
}

const obServicePortEnv = "OB_SERVICE_PORT"

var config *Config

func NewConfig(properties map[string]string) (*Config, error) {
	mysqlConfig, err := mysqlengine.NewConfig(properties)
	if err != nil {
		return nil, err
	}
	if mysqlConfig.Username == "" {
		mysqlConfig.Username = "root"
	}
	if viper.IsSet(obServicePortEnv) {
		mysqlConfig.Port = viper.GetString(obServicePortEnv)
	} else {
		mysqlConfig.Port = "2881"
	}

	mysqlConfig.Password = getRootPassword("")
	config = &Config{
		Config: mysqlConfig,
	}
	return config, nil
}

func getRootPassword(compName string) string {
	rootPasswordEnv := "OB_ROOT_PASSWD"
	// to support different root password for different components
	rootPasswordEnvPerCmp := ""
	if compName == "" {
		compName = viper.GetString("KB_COMP_NAME")
	}

	if compName != "" {
		compName = strings.ToUpper(compName)
		compName = strings.ReplaceAll(compName, "-", "_")
		rootPasswordEnvPerCmp = rootPasswordEnv + "_" + compName
	}

	if viper.IsSet(rootPasswordEnvPerCmp) {
		rootPasswordEnv = rootPasswordEnvPerCmp
	}
	return viper.GetString(rootPasswordEnv)
}
