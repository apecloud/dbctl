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

package replica

import (
	"context"

	"github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/apecloud/dbctl/engines"
	"github.com/apecloud/dbctl/engines/register"
	"github.com/apecloud/dbctl/operations"
	"github.com/apecloud/dbctl/util"
)

type GetRole struct {
	operations.Base
	DBManager engines.DBManager
}

var getrole operations.Operation = &GetRole{}

func init() {
	err := operations.Register("getrole", getrole)
	if err != nil {
		panic(err.Error())
	}
}

func (s *GetRole) Init(ctx context.Context) error {
	s.Logger = ctrl.Log.WithName("getrole")
	dbManager, err := register.GetDBManager()
	if err != nil {
		return errors.Wrap(err, "get manager failed")
	}

	s.DBManager = dbManager
	return nil
}

func (s *GetRole) IsReadonly(ctx context.Context) bool {
	return true
}

func (s *GetRole) Do(ctx context.Context, req *operations.OpsRequest) (*operations.OpsResponse, error) {
	resp := &operations.OpsResponse{
		Data: map[string]any{},
	}
	resp.Data["operation"] = util.GetRoleOperation

	role, err := s.DBManager.GetReplicaRole(ctx)
	if err != nil {
		s.Logger.Info("executing getrole error", "error", err)
		return resp, err
	}

	resp.Data["role"] = role
	return resp, err
}
