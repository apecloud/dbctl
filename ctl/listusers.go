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

	"github.com/apecloud/dbctl/operations/user"
	"github.com/apecloud/dbctl/util"
)

var (
	lorryAddr string
)

type ListUsersOptions struct {
	OptionsBase
}

func (options *ListUsersOptions) Validate() error {
	return options.PreCheck(context.Background(), nil)
}

func (options *ListUsersOptions) Run() error {
	listUsers, ok := options.Operation.(*user.ListUsers)
	if !ok {
		return errors.Errorf("%s operation not found", options.Action)
	}

	users, err := listUsers.DBManager.ListUsers(context.Background())
	if err != nil {
		return errors.Wrap(err, "executing listUsers failed")
	}
	fmt.Printf("list users:\n")
	for _, u := range users {
		fmt.Println("-------------------------")
		fmt.Printf("name: %s\n", u.UserName)
		fmt.Printf("role: %s\n", u.RoleName)
	}
	return nil
}

var listUsersOptions = &ListUsersOptions{
	OptionsBase: OptionsBase{
		Action: string(util.ListUsersOp),
	},
}

var ListUsersCmd = &cobra.Command{
	Use:   "listusers",
	Short: "list normal users.",
	Example: `
dbctl database listusers 
  `,
	Args: cobra.MinimumNArgs(0),
	Run:  CmdRunner(listUsersOptions),
}

func init() {
	ListUsersCmd.Flags().StringVarP(&lorryAddr, "lorry-addr", "", "http://localhost:3501/v1.0/", "The addr of lorry to request")
	ListUsersCmd.Flags().BoolP("help", "h", false, "Print this help message")

	DatabaseCmd.AddCommand(ListUsersCmd)
}
