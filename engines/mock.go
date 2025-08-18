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

package engines

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
)

type MockManager struct {
	DBManagerBase
}

var _ DBManager = &MockManager{}

func NewMockManager(properties Properties) (DBManager, error) {
	logger := ctrl.Log.WithName("MockManager")

	managerBase, err := NewDBManagerBase(logger)
	if err != nil {
		return nil, err
	}

	Mgr := &MockManager{
		DBManagerBase: *managerBase,
	}

	return Mgr, nil
}

func (*MockManager) IsDBStartupReady() bool {
	return true
}

func (*MockManager) Lock(context.Context, string) error {
	return fmt.Errorf("NotSupported")
}

func (*MockManager) Unlock(context.Context) error {
	return fmt.Errorf("NotSupported")
}
