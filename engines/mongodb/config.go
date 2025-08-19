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

package mongodb

import (
	"net"
	"strconv"
	"time"

	"github.com/spf13/viper"

	"github.com/apecloud/dbctl/constant"
	utilconfig "github.com/apecloud/dbctl/util/config"
)

const (
	adminDatabase = "admin"

	defaultTimeout             = 5 * time.Second
	defaultDBPort              = 27017
	UserEnv                    = "MONGODB_USER"
	PasswordEnv                = "MONGODB_PASSWORD"
	RootUserEnv                = "MONGODB_ROOT_USER"
	RootPasswordEnv            = "MONGODB_ROOT_PASSWORD"
	ClusterRoleEnv             = "MONGODB_CLUSTER_ROLE"
	GrantAnyActionPrivilegeEnv = "MONGODB_GRANT_ANYACTION_PRIVILEGE"
)

type Config struct {
	Hosts                   []string
	Username                string
	Password                string
	ReplSetName             string
	DatabaseName            string
	Params                  string
	Direct                  bool
	OperationTimeout        time.Duration
	ConfigSvr               bool
	GrantAnyActionPrivilege bool
}

var config *Config

func NewConfig() (*Config, error) {
	config = &Config{
		Direct:           true,
		Username:         "root",
		Hosts:            []string{"127.0.0.1:27017"},
		Params:           "?directConnection=true",
		OperationTimeout: defaultTimeout,
	}

	if viper.IsSet(constant.KBEnvServicePort) {
		config.Hosts = []string{"localhost:" + viper.GetString(constant.KBEnvServicePort)}
	}

	_ = viper.BindEnv(constant.ConfigKeyUserName, constant.KBEnvServiceUser, RootUserEnv, UserEnv)
	if viper.IsSet(constant.ConfigKeyUserName) {
		config.Username = viper.GetString(constant.ConfigKeyUserName)
	}

	_ = viper.BindEnv(constant.ConfigKeyPassword, constant.KBEnvServicePassword, RootPasswordEnv, PasswordEnv)
	if viper.IsSet(constant.ConfigKeyPassword) {
		config.Password = viper.GetString(constant.ConfigKeyPassword)
	}

	if viper.IsSet(ClusterRoleEnv) {
		config.ConfigSvr = viper.GetString(ClusterRoleEnv) == "configsvr"
	}
	if viper.IsSet(GrantAnyActionPrivilegeEnv) {
		config.GrantAnyActionPrivilege = viper.GetBool(GrantAnyActionPrivilegeEnv)
	}
	config.ReplSetName = constant.GetClusterCompName()
	config.DatabaseName = adminDatabase

	return config, nil
}

func (config *Config) GetDBPort() int {
	_, portStr, err := net.SplitHostPort(config.Hosts[0])
	if err != nil {
		return defaultDBPort
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return defaultDBPort
	}

	return port
}

func (config *Config) DeepCopy() *Config {
	newConf, _ := utilconfig.Clone(config)
	return newConf.(*Config)
}
