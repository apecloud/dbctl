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
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	ConnectionURLKey = "url"
	DefaultPort      = 5432
	EnvRootUser      = "POSTGRES_USER"
	EnvRootPassword  = "POSTGRES_PASSWORD"

	DefaultUrl                  = "user=postgres password=docker host=localhost port=5432 dbname=postgres pool_min_conns=1 pool_max_conns=10"
	DefaultMaxConnectionTimeout = "5"
)

type Config struct {
	url            string
	username       string
	password       string
	host           string
	port           int
	database       string
	maxConnections int32
	minConnections int32
	connectTimeout string
	pgxConfig      *pgxpool.Config
}

var config *Config

func NewConfig() (*Config, error) {
	config = &Config{}

	poolConfig, err := pgxpool.ParseConfig(DefaultUrl)
	if err != nil {
		return nil, errors.Errorf("error opening DB connection: %v", err)
	}

	config.username = poolConfig.ConnConfig.User
	config.password = poolConfig.ConnConfig.Password
	config.host = poolConfig.ConnConfig.Host
	config.port = int(poolConfig.ConnConfig.Port)
	config.database = poolConfig.ConnConfig.Database
	config.maxConnections = poolConfig.MaxConns
	config.minConnections = poolConfig.MinConns
	config.connectTimeout = DefaultMaxConnectionTimeout

	if viper.IsSet(EnvRootUser) {
		config.username = viper.GetString(EnvRootUser)
	}
	if viper.IsSet(EnvRootPassword) {
		config.password = viper.GetString(EnvRootPassword)
	}

	config.url = config.GetConnectURLWithHost(config.host)
	pgxConfig, _ := pgxpool.ParseConfig(config.url)
	config.pgxConfig = pgxConfig

	return config, nil
}

func (config *Config) GetDBPort() int {
	if config.port == 0 {
		return DefaultPort
	}

	return config.port
}

func (config *Config) GetConnectURLWithHost(host string) string {
	return fmt.Sprintf("user=%s password=%s host=%s port=%d dbname=%s",
		config.username, config.password, host, config.port, config.database)
}
