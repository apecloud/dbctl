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

package constant

import (
	"os"

	"github.com/spf13/viper"
)

// Lorry
const (
	KBEnvEngineType      = "KB_ENGINE_TYPE"
	KBEnvServiceUser     = "KB_SERVICE_USER"
	KBEnvServicePassword = "KB_SERVICE_PASSWORD"
	// KBEnvServiceRoles defines the Roles configured in the cluster definition that are visible to users.
	KBEnvServiceRoles = "KB_SERVICE_ROLES"

	// KBEnvServicePort defines the port of the DB service
	KBEnvServicePort = "KB_SERVICE_PORT"
)

// new envs for KB 1.0
const (
	EnvPodName         = "MY_POD_NAME"
	EnvClusterCompName = "MY_CLUSTER_COMP_NAME"
)

// old envs for KB 0.9
const (
	KBEnvNamespace       = "KB_NAMESPACE"
	KBEnvClusterCompName = "KB_CLUSTER_COMP_NAME"
	KBEnvPodName         = "KB_POD_NAME"
)

func GetPodName() string {
	switch {
	case viper.IsSet(KBEnvPodName):
		return viper.GetString(KBEnvPodName)
	case viper.IsSet(EnvPodName):
		return viper.GetString(EnvPodName)
	default:
		// this may be not correct in some cases, like in the case of host network, it will return the node hostname instead of pod name.
		podName, _ := os.Hostname()
		return podName
	}
}

func GetClusterCompName() string {
	switch {
	case viper.IsSet(KBEnvClusterCompName):
		return viper.GetString(KBEnvClusterCompName)
	case viper.IsSet(EnvClusterCompName):
		return viper.GetString(EnvClusterCompName)
	default:
		return ""
	}
}

const (
	ConfigKeyUserName = "username"
	ConfigKeyPassword = "password"
)
