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

package util

import (
	"errors"
	"strings"
)

type OperationKind string

const (
	RespFieldEvent   = "event"
	RespFieldMessage = "message"
	RespTypMeta      = "metadata"
	RespEveSucc      = "Success"
	RespEveFail      = "Failed"

	GetOperation    OperationKind = "get"
	CreateOperation OperationKind = "create"
	DeleteOperation OperationKind = "delete"
	ListOperation   OperationKind = "list"

	CheckRunningOperation OperationKind = "checkRunning"
	HealthyCheckOperation OperationKind = "healthyCheck"
	CheckRoleOperation    OperationKind = "checkRole"
	GetRoleOperation      OperationKind = "getRole"
	GetLagOperation       OperationKind = "getLag"
	SwitchoverOperation   OperationKind = "switchover"
	ExecOperation         OperationKind = "exec"
	QueryOperation        OperationKind = "query"
	CloseOperation        OperationKind = "close"

	LockOperation    OperationKind = "lockInstance"
	UnlockOperation  OperationKind = "unlockInstance"
	VolumeProtection OperationKind = "volumeProtection"

	// for component
	PostProvisionOperation OperationKind = "postProvision"
	PreTerminateOperation  OperationKind = "preTerminate"

	DataDumpOperation OperationKind = "dataDump"
	DataLoadOperation OperationKind = "dataLoad"

	OperationNotImplemented    = "NotImplemented"
	OperationInvalid           = "Invalid"
	OperationSuccess           = "Success"
	OperationFailed            = "Failed"
	DefaultProbeTimeoutSeconds = 2
)

type RoleType string

func (r RoleType) EqualTo(role string) bool {
	return strings.EqualFold(string(r), role)
}

func (r RoleType) GetWeight() int32 {
	switch r {
	case SuperUserRole:
		return 1 << 3
	case ReadWriteRole:
		return 1 << 2
	case ReadOnlyRole:
		return 1 << 1
	case CustomizedRole:
		return 1
	default:
		return 0
	}
}

const (
	SuperUserRole  RoleType = "superuser"
	ReadWriteRole  RoleType = "readwrite"
	ReadOnlyRole   RoleType = "readonly"
	NoPrivileges   RoleType = ""
	CustomizedRole RoleType = "customized"
	InvalidRole    RoleType = "invalid"
)

// ProbeError is the error for Lorry probe api, it implements error interface
type ProbeError struct {
	message string
}

var _ error = ProbeError{}

func (e ProbeError) Error() string {
	return e.message
}

func NewProbeError(msg string) error {
	return ProbeError{
		message: msg,
	}
}

var ErrNotImplemented = errors.New("not implemented")
