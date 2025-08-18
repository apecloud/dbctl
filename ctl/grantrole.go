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

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/apecloud/dbctl/operations"
	"github.com/apecloud/dbctl/operations/user"
	"github.com/apecloud/dbctl/util"
)

type GrantUserRoleOptions struct {
	OptionsBase
	userName string
	roleName string
}

func (options *GrantUserRoleOptions) Validate() error {
	parameters := map[string]any{
		"userName": options.userName,
		"roleName": options.roleName,
	}

	req := &operations.OpsRequest{
		Parameters: parameters,
	}
	options.Request = req
	return options.PreCheck(context.Background(), req)
}

func (options *GrantUserRoleOptions) Run() error {
	grantUser, ok := options.Operation.(*user.GrantRole)
	if !ok {
		return errors.New("grantUser operation not found")
	}

	_, err := grantUser.Do(context.Background(), options.Request)
	if err != nil {
		return errors.Wrap(err, "executing grantUser failed")
	}
	return nil
}

var grantUserRoleOptions = &GrantUserRoleOptions{
	OptionsBase: OptionsBase{
		Action: string(util.GrantUserRoleOp),
	},
}

var GrantUserRoleCmd = &cobra.Command{
	Use:   "grant-role",
	Short: "grant user role.",
	Example: `
dbctl database grant-role --username xxx --rolename xxx 
  `,
	Args: cobra.MinimumNArgs(0),
	Run:  CmdRunner(grantUserRoleOptions),
}

func init() {
	GrantUserRoleCmd.Flags().StringVarP(&grantUserRoleOptions.userName, "username", "", "", "The name of user to grant")
	GrantUserRoleCmd.Flags().StringVarP(&grantUserRoleOptions.roleName, "rolename", "", "", "The name of role to grant")
	GrantUserRoleCmd.Flags().BoolP("help", "h", false, "Print this help message")

	DatabaseCmd.AddCommand(GrantUserRoleCmd)
}
