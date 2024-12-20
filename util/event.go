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

package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
	ctlruntime "sigs.k8s.io/controller-runtime"

	workloads "github.com/apecloud/kubeblocks/apis/workloads/v1alpha1"

	"github.com/apecloud/dbctl/constant"
)

var logger = ctlruntime.Log.WithName("event")

func SentEventForProbe(ctx context.Context, data map[string]any) error {
	logger.Info(fmt.Sprintf("send event: %v", data))
	roleUpdateMechanism := workloads.DirectAPIServerEventUpdate
	if viper.IsSet(constant.KBEnvRsmRoleUpdateMechanism) {
		roleUpdateMechanism = workloads.RoleUpdateMechanism(viper.GetString(constant.KBEnvRsmRoleUpdateMechanism))
	}

	switch roleUpdateMechanism {
	case workloads.ReadinessProbeEventUpdate:
		return NewProbeError("not sending event directly, use readiness probe instand")
	case workloads.DirectAPIServerEventUpdate:
		operation, ok := data["operation"]
		if !ok {
			return errors.New("operation failed must be set")
		}
		event, err := CreateEvent(string(operation.(OperationKind)), data)
		if err != nil {
			logger.Info("generate event failed", "error", err.Error())
			return err
		}

		go func() {
			_ = SendEvent(ctx, event)
		}()
	default:
		logger.Info(fmt.Sprintf("no event sent, RoleUpdateMechanism: %s", roleUpdateMechanism))
	}

	return nil
}

func CreateEvent(reason string, data map[string]any) (*corev1.Event, error) {
	// get pod object
	podName := constant.GetPodName()
	podUID := constant.GetPodUID()
	nodeName := viper.GetString(constant.KBEnvNodeName)
	namespace := constant.GetNamespace()
	msg, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s.%s", podName, rand.String(16)),
			Namespace: namespace,
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
			FieldPath: "spec.containers{lorry}",
		},
		Reason:  reason,
		Message: string(msg),
		Source: corev1.EventSource{
			Component: "lorry",
			Host:      nodeName,
		},
		FirstTimestamp:      metav1.Now(),
		LastTimestamp:       metav1.Now(),
		EventTime:           metav1.NowMicro(),
		ReportingController: "lorry",
		ReportingInstance:   podName,
		Action:              reason,
		Type:                "Normal",
	}
	return event, nil
}

func SendEvent(ctx context.Context, event *corev1.Event) error {
	ctx1 := context.Background()
	config, err := ctlruntime.GetConfig()
	if err != nil {
		logger.Info("get k8s client config failed", "error", err.Error())
		return err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Info("k8s client create failed", "error", err.Error())
		return err
	}
	namespace := constant.GetNamespace()
	for i := 0; i < 30; i++ {
		_, err = clientset.CoreV1().Events(namespace).Create(ctx1, event, metav1.CreateOptions{})
		if err == nil {
			logger.Info("send event success", "message", event.Message)
			break
		}
		logger.Info("send event failed", "error", err.Error())
		time.Sleep(10 * time.Second)
	}
	return err
}
