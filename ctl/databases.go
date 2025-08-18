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
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/apecloud/dbctl/constant"
	"github.com/apecloud/dbctl/dcs"
	"github.com/apecloud/dbctl/engines/models"
	"github.com/apecloud/dbctl/engines/register"
)

const ()

var DatabaseCmd = &cobra.Command{
	Use:     "database",
	Aliases: models.GetEngineTypeListStr(),
	Short:   "specify database.",
	Example: `
dbctl mongodb createuser --username root --password password
  `,
	Args: cobra.MinimumNArgs(0),
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		commands := stripFlags(os.Args[1:], cmd)
		// fmt.Println("commands: ", commands)
		if len(commands) < 1 {
			return errors.New("please specify a database subcommand")
		}
		dbType := commands[0]
		if dbType == "database" {
			return errors.New("please specify a database type supported by dbctl, the valid types are: " + strings.Join(models.GetEngineTypeListStr(), ", "))
		}
		viper.SetDefault(constant.KBEnvEngineType, commands[0])

		// Initialize DCS (Distributed Control System)
		_ = dcs.InitStore()
		// Initialize DB Manager
		err := register.InitDBManager(configDir, "")
		if err != nil {
			return errors.Wrap(err, "DB manager initialize failed")
		}
		return nil
	},

	Run: func(cmd *cobra.Command, _ []string) {
		_ = cmd.Help()
	},
}

func init() {
	RootCmd.AddCommand(DatabaseCmd)
}

func hasNoOptDefVal(name string, fs *pflag.FlagSet) bool {
	flag := fs.Lookup(name)
	if flag == nil {
		return false
	}
	return flag.NoOptDefVal != ""
}

func shortHasNoOptDefVal(name string, fs *pflag.FlagSet) bool {
	if len(name) == 0 {
		return false
	}

	flag := fs.ShorthandLookup(name[:1])
	if flag == nil {
		return false
	}
	return flag.NoOptDefVal != ""
}

func stripFlags(args []string, c *cobra.Command) []string {
	if len(args) == 0 {
		return args
	}

	commands := []string{}
	flags := c.Flags()

Loop:
	for len(args) > 0 {
		s := args[0]
		args = args[1:]
		switch {
		case s == "--":
			// "--" terminates the flags
			break Loop
		case strings.HasPrefix(s, "--") && !strings.Contains(s, "=") && !hasNoOptDefVal(s[2:], flags):
			// If '--flag arg' then
			// delete arg from args.
			fallthrough // (do the same as below)
		case strings.HasPrefix(s, "-") && !strings.Contains(s, "=") && len(s) == 2 && !shortHasNoOptDefVal(s[1:], flags):
			// If '-f arg' then
			// delete 'arg' from args or break the loop if len(args) <= 1.
			if len(args) <= 1 {
				break Loop
			} else {
				args = args[1:]
				continue
			}
		case s != "" && !strings.HasPrefix(s, "-"):
			commands = append(commands, s)
		}
	}

	return commands
}
