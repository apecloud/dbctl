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

package dcs

import (
	"github.com/spf13/viper"

	"github.com/apecloud/dbctl/constant"
)

type DCS interface {
	Initialize() error

	// cluster manage functions
	GetClusterName() string
	GetCluster() (*Cluster, error)
	GetClusterFromCache() *Cluster
	ResetCluster()
	DeleteCluster()

	// cluster scole ha config
	GetHaConfig() (*HaConfig, error)
	UpdateHaConfig() error

	// member manager functions
	GetMembers() ([]Member, error)
	AddCurrentMember() error

	// manual switchover
	GetSwitchover() (*Switchover, error)
	CreateSwitchover(string, string) error
	DeleteSwitchover() error

	// cluster scope leader lock
	AttemptAcquireLease() error
	CreateLease() error
	IsLeaseExist() (bool, error)
	HasLease() bool
	ReleaseLease() error
	UpdateLease() error

	GetLeader() (*Leader, error)
}

var dcs DCS

func init() {
	viper.SetDefault(constant.KBEnvTTL, 15)
	viper.SetDefault(constant.KBEnvMaxLag, 10)
	viper.SetDefault(constant.KubernetesClusterDomainEnv, constant.DefaultDNSDomain)
}

func SetStore(d DCS) {
	dcs = d
}

// fixme: this is a temporary solution to avoid error log
func GetStore() DCS {
	return &KubernetesStore{}
}

func InitStore() error {
	//store, err := NewKubernetesStore()
	//if err != nil {
	//	return err
	//}
	dcs = &KubernetesStore{}
	return nil
}
