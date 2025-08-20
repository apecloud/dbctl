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

type OperationKind string

const (
	RespFieldEvent   = "event"
	RespFieldMessage = "message"

	ExecOperation    OperationKind = "exec"
	QueryOperation   OperationKind = "query"
	GetRoleOperation OperationKind = "getRole"

	OperationSuccess = "Success"
	OperationFailed  = "Failed"
)

// ProbeError is the error for dbctl probe api, it implements error interface
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
