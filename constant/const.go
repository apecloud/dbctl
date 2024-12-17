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

package constant

const (
	// AppInstanceLabelKey refer cluster.Name
	AppInstanceLabelKey    = "app.kubernetes.io/instance"
	AppManagedByLabelKey   = "app.kubernetes.io/managed-by"
	RoleLabelKey           = "kubeblocks.io/role" // RoleLabelKey consensusSet and replicationSet role label key
	KBAppComponentLabelKey = "apps.kubeblocks.io/component-name"
)

const (
	LorryHTTPPortName = "lorry-http-port"
)

const (
	KubernetesClusterDomainEnv = "KUBERNETES_CLUSTER_DOMAIN"
	DefaultDNSDomain           = "cluster.local"
)
