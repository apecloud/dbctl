/*
Copyright (C) 2022-2023 ApeCloud Co., Ltd

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

package etcd

import (
	"context"
	"fmt"
	"net"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/viper"

	"github.com/apecloud/dbctl/constant"
)

// Test case for Init() function
var _ = Describe("ETCD DBManager", func() {
	// Set up relevant viper config variables
	viper.Set(constant.KBEnvServiceUser, "testuser")
	viper.Set(constant.KBEnvServicePassword, "testpassword")
	Context("new db manager", func() {
		It("with right configurations", func() {
			dbManger, err := NewManager()
			Expect(err).Should(Succeed())
			Expect(dbManger).ShouldNot(BeNil())
		})
	})

	Context("is db startup ready", func() {
		It("it is ready", func() {
			etcdServer, err := StartEtcdServer()
			Expect(err).Should(BeNil())
			defer etcdServer.Stop()
			testEndpoint := fmt.Sprintf("http://%s", etcdServer.ETCD.Clients[0].Addr().(*net.TCPAddr).String())
			manager := &Manager{
				etcd:     etcdServer.client,
				endpoint: testEndpoint,
			}
			Expect(manager.IsDBStartupReady()).Should(BeTrue())
		})

		It("it is not ready", func() {
			etcdServer, err := StartEtcdServer()
			Expect(err).Should(BeNil())
			etcdServer.Stop()
			manager, err := NewManager()
			Expect(err).Should(BeNil())
			Expect(manager).ShouldNot(BeNil())
			Expect(manager.IsDBStartupReady()).Should(BeFalse())
		})
	})

	Context("get replica role", func() {
		It("get leader", func() {
			etcdServer, err := StartEtcdServer()
			Expect(err).Should(BeNil())
			defer etcdServer.Stop()
			testEndpoint := fmt.Sprintf("http://%s", etcdServer.ETCD.Clients[0].Addr().(*net.TCPAddr).String())
			manager := &Manager{
				etcd:     etcdServer.client,
				endpoint: testEndpoint,
			}
			role, err := manager.GetReplicaRole(context.Background())
			Expect(err).Should(BeNil())
			Expect(role).Should(Equal("leader"))
		})
	})
})
