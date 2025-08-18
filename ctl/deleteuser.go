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

type DeleteUserOptions struct {
	OptionsBase
	userName string
}

func (options *DeleteUserOptions) Validate() error {
	parameters := map[string]any{
		"userName": options.userName,
	}
	req := &operations.OpsRequest{
		Parameters: parameters,
	}
	options.Request = req
	return options.PreCheck(context.Background(), req)
}

func (options *DeleteUserOptions) Run() error {
	deleteUser, ok := options.Operation.(*user.DeleteUser)
	if !ok {
		return errors.New("createUser operation not found")
	}

	_, err := deleteUser.Do(context.Background(), options.Request)
	if err != nil {
		return errors.Wrap(err, "executing deleteUser failed")
	}
	return nil
}

var deleteUserOptions = &DeleteUserOptions{
	OptionsBase: OptionsBase{
		Action: string(util.DeleteUserOp),
	},
}

var DeleteUserCmd = &cobra.Command{
	Use:   "deleteuser",
	Short: "delete user.",
	Example: `
dbctl database deleteuser --username xxx 
  `,
	Args: cobra.MinimumNArgs(0),
	Run:  CmdRunner(deleteUserOptions),
}

func init() {
	DeleteUserCmd.Flags().StringVarP(&deleteUserOptions.userName, "username", "", "", "The name of user to delete")
	DeleteUserCmd.Flags().BoolP("help", "h", false, "Print this help message")

	DatabaseCmd.AddCommand(DeleteUserCmd)
}
