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
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/spf13/viper"

	"github.com/apecloud/dbctl/constant"
)

const (
	// configurations to connect to MySQL, either a data source name represent by URL.
	connectionURLKey = "url"
)

const (
	adminDatabase = "mysql"
	defaultDBPort = 3306
	EnvRootUser   = "MYSQL_ROOT_USER"
	EnvRootPass   = "MYSQL_ROOT_PASSWORD"
)

type Config struct {
	URL                 string
	Port                string
	Username            string
	Password            string
	pemPath             string
	MaxIdleConns        int
	MaxOpenConns        int
	AdminUsername       string
	AdminPassword       string
	ReplicationUsername string
	ReplicationPassword string
}

var fs = afero.NewOsFs()

var config *Config

func NewConfig() (*Config, error) {
	config = &Config{
		URL:          "root:@tcp(127.0.0.1:3306)/mysql?multiStatements=true",
		MaxIdleConns: 1,
		MaxOpenConns: 5,
	}

	config.Username = getRootUserName()
	config.Password = getRootPassword()
	config.AdminUsername = getAdminUserName()
	config.AdminPassword = getAdminPassword()
	config.ReplicationUsername = getReplicationUserName()
	config.ReplicationPassword = getReplicationPassword()

	if viper.IsSet(constant.KBEnvServicePort) {
		config.Port = viper.GetString(constant.KBEnvServicePort)
	}

	if config.pemPath != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := afero.ReadFile(fs, config.pemPath)
		if err != nil {
			return nil, errors.Wrapf(err, "Error reading PEM file from %s", config.pemPath)
		}

		ok := rootCertPool.AppendCertsFromPEM(pem)
		if !ok {
			return nil, fmt.Errorf("failed to append PEM")
		}

		err = mysql.RegisterTLSConfig("custom", &tls.Config{RootCAs: rootCertPool, MinVersion: tls.VersionTLS12})
		if err != nil {
			return nil, errors.Wrap(err, "Error register TLS config")
		}
	}
	return config, nil
}

func getRootUserName() string {
	if viper.IsSet(constant.KBEnvServiceUser) {
		return viper.GetString(constant.KBEnvServiceUser)
	} else if viper.IsSet(EnvRootUser) {
		return viper.GetString(EnvRootUser)
	}
	return ""
}

func getRootPassword() string {
	if viper.IsSet(constant.KBEnvServicePassword) {
		return viper.GetString(constant.KBEnvServicePassword)
	} else if viper.IsSet(EnvRootPass) {
		return viper.GetString(EnvRootPass)
	}
	return ""
}

func getAdminUserName() string {
	// if the user is not set, use the root user
	if viper.IsSet("MYSQL_ADMIN_USER") {
		return viper.GetString("MYSQL_ADMIN_USER")
	}
	return getRootUserName()
}

func getAdminPassword() string {
	// if the password is not set, use the root password
	if viper.IsSet("MYSQL_ADMIN_PASSWORD") {
		return viper.GetString("MYSQL_ADMIN_PASSWORD")
	}
	return getRootPassword()
}

func getReplicationUserName() string {
	// if the user is not set, use the admin user
	if viper.IsSet("MYSQL_REPLICATION_USER") {
		return viper.GetString("MYSQL_REPLICATION_USER")
	}
	return getAdminUserName()
}

func getReplicationPassword() string {
	// if the password is not set, use the admin password
	if viper.IsSet("MYSQL_REPLICATION_PASSWORD") {
		return viper.GetString("MYSQL_REPLICATION_PASSWORD")
	}
	return getAdminPassword()
}

func (config *Config) GetLocalDBConn() (*sql.DB, error) {
	mysqlConfig, err := mysql.ParseDSN(config.URL)
	if err != nil {
		return nil, errors.Wrapf(err, "illegal Data Source Name (DNS) specified by %s", connectionURLKey)
	}
	mysqlConfig.User = config.Username
	mysqlConfig.Passwd = config.Password
	mysqlConfig.Timeout = time.Second * 5
	mysqlConfig.ReadTimeout = time.Second * 5
	mysqlConfig.WriteTimeout = time.Second * 5
	if config.Port != "" {
		mysqlConfig.Addr = "127.0.0.1:" + config.Port
	}
	db, err := GetDBConnection(mysqlConfig.FormatDSN())
	if err != nil {
		return nil, errors.Wrap(err, "get DB connection failed")
	}

	return db, nil
}
