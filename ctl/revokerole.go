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

	"github.com/apecloud/dbctl/operations/user"
	"github.com/apecloud/dbctl/util"
)

type RevokeUserRoleOptions struct {
	OptionsBase
	userName string
	roleName string
}

func (options *RevokeUserRoleOptions) Validate() error {
	return options.PreCheck(context.Background(), nil)
}

func (options *RevokeUserRoleOptions) Run() error {
	revokeUserRole, ok := options.Operation.(*user.RevokeRole)
	if !ok {
		return errors.Errorf("%s operation not found", options.Action)
	}

	_, err := revokeUserRole.Do(context.Background(), options.Request)
	if err != nil {
		return errors.Wrap(err, "executing revokeUserRole failed")
	}
	return nil
}

var revokeUserRoleOptions = &RevokeUserRoleOptions{
	OptionsBase: OptionsBase{
		Action: string(util.RevokeUserRoleOp),
	},
}

var RevokeUserRoleCmd = &cobra.Command{
	Use:   "revoke-role",
	Short: "revoke user role.",
	Example: `
dbctl database revoke-role --username xxx --rolename xxx 
  `,
	Args: cobra.MinimumNArgs(0),
	Run:  CmdRunner(revokeUserRoleOptions),
}

func init() {
	RevokeUserRoleCmd.Flags().StringVarP(&revokeUserRoleOptions.userName, "username", "", "", "The name of user to revoke")
	RevokeUserRoleCmd.Flags().StringVarP(&revokeUserRoleOptions.roleName, "rolename", "", "", "The name of role to revoke")
	RevokeUserRoleCmd.Flags().BoolP("help", "h", false, "Print this help message")

	DatabaseCmd.AddCommand(RevokeUserRoleCmd)
}
