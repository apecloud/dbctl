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

type ListSystemAccountsOptions struct {
	OptionsBase
}

func (options *ListSystemAccountsOptions) Validate() error {
	return options.PreCheck(context.Background(), nil)
}

func (options *ListSystemAccountsOptions) Run() error {
	listSystemAccounts, ok := options.Operation.(*user.ListSystemAccounts)
	if !ok {
		return errors.Errorf("%s operation not found", options.Action)
	}

	users, err := listSystemAccounts.DBManager.ListSystemAccounts(context.Background())
	if err != nil {
		return errors.Wrap(err, "executing listSystemAccounts failed")
	}
	fmt.Printf("list users:\n")
	for _, u := range users {
		fmt.Println("-------------------------")
		fmt.Printf("name: %s\n", u.UserName)
		fmt.Printf("role: %s\n", u.RoleName)
	}
	return nil
}

var listSystemAccountsOptions = &ListSystemAccountsOptions{
	OptionsBase: OptionsBase{
		Action: string(util.ListSystemAccountsOp),
	},
}

var ListSystemAccountsCmd = &cobra.Command{
	Use:   "listsystemaccounts",
	Short: "list system accounts.",
	Example: `
dbctl database listsystemaccounts 
  `,
	Args: cobra.MinimumNArgs(0),
	Run:  CmdRunner(listSystemAccountsOptions),
}

func init() {
	ListSystemAccountsCmd.Flags().BoolP("help", "h", false, "Print this help message")

	DatabaseCmd.AddCommand(ListSystemAccountsCmd)
}
