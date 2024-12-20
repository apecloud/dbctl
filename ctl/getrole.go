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

package ctl

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/apecloud/dbctl/operations/replica"
)

type GetRoleOptions struct {
	OptionsBase
}

func (options *GetRoleOptions) Run() error {
	getRole, ok := getRoleOptions.Operation.(*replica.GetRole)
	if !ok {
		return errors.New("getrole operation not found")

	}

	role, err := getRole.DBManager.GetReplicaRole(context.Background())
	if err != nil {
		return errors.Wrap(err, "executing getrole failed")
	}
	fmt.Print(role)
	return nil
}

var getRoleOptions = &GetRoleOptions{
	OptionsBase: OptionsBase{
		Action: "getrole",
	},
}

var GetRoleCmd = &cobra.Command{
	Use:   "getrole",
	Short: "get role of the replica.",
	Example: `
dbctl database getrole 
  `,
	Args: cobra.MinimumNArgs(0),
	Run:  CmdRunner(getRoleOptions),
}

func init() {
	GetRoleCmd.Flags().BoolP("help", "h", false, "Print this help message")

	DatabaseCmd.AddCommand(GetRoleCmd)
}
