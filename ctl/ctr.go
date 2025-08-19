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
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	kzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const cliVersionTemplateString = "CLI version: %s \nRuntime version: %s\n"

var opts = kzap.Options{
	Development: true,
	Level:       zap.NewAtomicLevelAt(zap.DPanicLevel),
}

var RootCmd = &cobra.Command{
	Use:   "dbctl",
	Short: "dbctl command line interface",
	Long: `
_                 ______   _______  ______   _        _______  _______  _        _______    ______   ______   _______ _________ _       
| \    /\|\     /|(  ___ \ (  ____ \(  ___ \ ( \      (  ___  )(  ____ \| \    /\(  ____ \  (  __  \ (  ___ \ (  ____ \\__   __/( \      
|  \  / /| )   ( || (   ) )| (    \/| (   ) )| (      | (   ) || (    \/|  \  / /| (    \/  | (  \  )| (   ) )| (    \/   ) (   | (      
|  (_/ / | |   | || (__/ / | (__    | (__/ / | |      | |   | || |      |  (_/ / | (_____   | |   ) || (__/ / | |         | |   | |      
|   _ (  | |   | ||  __ (  |  __)   |  __ (  | |      | |   | || |      |   _ (  (_____  )  | |   | ||  __ (  | |         | |   | |      
|  ( \ \ | |   | || (  \ \ | (      | (  \ \ | |      | |   | || |      |  ( \ \       ) |  | |   ) || (  \ \ | |         | |   | |      
|  /  \ \| (___) || )___) )| (____/\| )___) )| (____/\| (___) || (____/\|  /  \ \/\____) |  | (__/  )| )___) )| (____/\   | |   | (____/\
|_/    \/(_______)|/ \___/ (_______/|/ \___/ (_______/(_______)(_______/|_/    \/\_______)  (______/ |/ \___/ (_______/   )_(   (_______/
===============================
dbctl command line interface`,
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		err := viper.BindPFlags(pflag.CommandLine)
		if err != nil {
			return errors.Wrap(err, "fatal error viper bindPFlags")
		}

		// Initialize logger
		kopts := []kzap.Opts{kzap.UseFlagOptions(&opts)}
		if strings.EqualFold("debug", viper.GetString("zap-log-level")) {
			kopts = append(kopts, kzap.RawZapOpts(zap.AddCaller()))
		}
		ctrl.SetLogger(kzap.New(kopts...))

		return nil
	},

	Run: func(cmd *cobra.Command, _ []string) {
		if versionFlag {
			printVersion()
		} else {
			_ = cmd.Help()
		}
	},
}

type dbctlVersion struct {
	CliVersion     string `json:"Cli version"`
	RuntimeVersion string `json:"Runtime version"`
}

var (
	cliVersion       string
	versionFlag      bool
	dbctlVer         dbctlVersion
	dbctlRuntimePath string
)

// Execute adds all child commands to the root command.
func Execute(cliVersion, apiVersion string) {
	dbctlVer = dbctlVersion{
		CliVersion:     cliVersion,
		RuntimeVersion: apiVersion,
	}

	cobra.OnInitialize(initConfig)

	setVersion()

	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func setVersion() {
	template := fmt.Sprintf(cliVersionTemplateString, dbctlVer.CliVersion, dbctlVer.RuntimeVersion)
	RootCmd.SetVersionTemplate(template)
}

func printVersion() {
	fmt.Printf(cliVersionTemplateString, dbctlVer.CliVersion, dbctlVer.RuntimeVersion)
}

func initConfig() {
	// err intentionally ignored since dbctl may not yet be installed.
	runtimeVer := GetRuntimeVersion()

	dbctlVer = dbctlVersion{
		// Set in Execute() method in this file before initConfig() is called by cmd.Execute().
		CliVersion:     cliVersion,
		RuntimeVersion: strings.ReplaceAll(runtimeVer, "\n", ""),
	}
}

func init() {
	klog.InitFlags(flag.CommandLine)
	opts.BindFlags(flag.CommandLine)
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	RootCmd.PersistentFlags().StringVarP(&dbctlRuntimePath, "tools-dir", "", "/tools/", "The directory of tools binaries")
	RootCmd.PersistentFlags().AddFlagSet(pflag.CommandLine)
}

// GetRuntimeVersion returns the version for the local dbctl runtime.
func GetRuntimeVersion() string {
	// dbctlCMD := filepath.Join(dbctlRuntimePath, "dbctl")

	// out, err := exec.Command(dbctlCMD, "--version").Output()
	// if err != nil {
	// 	return "n/a\n"
	// }
	return string("v0.1.0")
}
