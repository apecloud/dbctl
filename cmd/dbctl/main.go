/*
Copyright (C) 2022-2025 ApeCloud Co., Ltd

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

package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/automaxprocs/maxprocs"
	"go.uber.org/zap"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	kzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/apecloud/dbctl/engines/register"
	"github.com/apecloud/dbctl/httpserver"
	opsregister "github.com/apecloud/dbctl/operations/register"
)

var disableDNSChecker bool
var engineType string

func init() {
	viper.AutomaticEnv()
	pflag.BoolVar(&disableDNSChecker, "disable-dns-checker", false, "disable dns checker, for test&dev")
	pflag.StringVar(&engineType, "engine", "", "Database engine type, e.g., mysql, postgres, mongodb, etc.")
}

func main() {
	_, _ = maxprocs.Set()

	// Initialize flags
	opts := kzap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	klog.InitFlags(nil)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		panic(errors.Wrap(err, "fatal error viper bindPFlags"))
	}

	// Initialize logger
	kOpts := []kzap.Opts{kzap.UseFlagOptions(&opts)}
	if strings.EqualFold("debug", viper.GetString("zap-log-level")) {
		kOpts = append(kOpts, kzap.RawZapOpts(zap.AddCaller()))
	}
	ctrl.SetLogger(kzap.New(kOpts...))

	// Initialize DB Manager
	err = register.InitDBManager(engineType)
	if err != nil {
		panic(errors.Wrap(err, "DB manager initialize failed"))
	}

	// start HTTP Server
	ops := opsregister.Operations()
	httpServer := httpserver.NewServer(ops)
	err = httpServer.StartNonBlocking()
	if err != nil {
		panic(errors.Wrap(err, "HTTP server initialize failed"))
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	<-stop
}
