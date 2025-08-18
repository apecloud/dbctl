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
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/apecloud/dbctl/httpserver"
	opsregister "github.com/apecloud/dbctl/operations/register"
)

var ServiceCmd = &cobra.Command{
	Use:   "service",
	Short: "Run dbctl as a daemon and provide api service.",
	Example: `
dbctl service
  `,
	Args: cobra.MinimumNArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		// start HTTP Server
		ops := opsregister.Operations()
		httpServer := httpserver.NewServer(ops)
		err := httpServer.StartNonBlocking()
		if err != nil {
			panic(errors.Wrap(err, "HTTP server initialize failed"))
		}

		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
		<-stop
	},
}

//func init() {
//	httpserver.InitFlags(ServiceCmd.Flags())
//	ServiceCmd.Flags().BoolP("help", "h", false, "Print this help message")
//
//	DatabaseCmd.AddCommand(ServiceCmd)
//}
