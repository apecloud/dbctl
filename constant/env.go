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
	KBEnvActionCommands  = "KB_ACTION_COMMANDS"
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
	EnvNamespace       = "MY_NAMESPACE"
	EnvPodName         = "MY_POD_NAME"
	EnvPodIP           = "MY_POD_IP"
	EnvPodUID          = "MY_POD_UID"
	EnvClusterName     = "MY_CLUSTER_NAME"
	EnvComponentName   = "MY_COMP_NAME"
	EnvClusterCompName = "MY_CLUSTER_COMP_NAME"
)

// old envs for KB 0.9
const (
	KBEnvNamespace       = "KB_NAMESPACE"
	KBEnvClusterName     = "KB_CLUSTER_NAME"
	KBEnvClusterCompName = "KB_CLUSTER_COMP_NAME"
	KBEnvCompName        = "KB_COMP_NAME"
	KBEnvPodName         = "KB_POD_NAME"
	KBEnvPodUID          = "KB_POD_UID"
	KBEnvPodIP           = "KB_POD_IP"
	KBEnvPodFQDN         = "KB_POD_FQDN"
	KBEnvNodeName        = "KB_NODENAME"
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

func GetPodIP() string {
	switch {
	case viper.IsSet(KBEnvPodIP):
		return viper.GetString(KBEnvPodIP)
	case viper.IsSet(EnvPodIP):
		return viper.GetString(EnvPodIP)
	default:
		return ""
	}
}

func GetPodUID() string {
	switch {
	case viper.IsSet(KBEnvPodUID):
		return viper.GetString(KBEnvPodUID)
	case viper.IsSet(EnvPodUID):
		return viper.GetString(EnvPodUID)
	default:
		return ""
	}
}

func GetNamespace() string {
	switch {
	case viper.IsSet(KBEnvNamespace):
		return viper.GetString(KBEnvNamespace)
	case viper.IsSet(EnvNamespace):
		return viper.GetString(EnvNamespace)
	default:
		return ""
	}
}

func GetClusterName() string {
	switch {
	case viper.IsSet(KBEnvClusterName):
		return viper.GetString(KBEnvClusterName)
	case viper.IsSet(EnvClusterName):
		return viper.GetString(EnvClusterName)
	default:
		return ""
	}
}

func GetComponentName() string {
	switch {
	case viper.IsSet(KBEnvCompName):
		return viper.GetString(KBEnvCompName)
	case viper.IsSet(EnvComponentName):
		return viper.GetString(EnvComponentName)
	default:
		return ""
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
