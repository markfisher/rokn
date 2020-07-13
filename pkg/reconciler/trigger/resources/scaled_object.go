package resources

import (
	"fmt"

	kedav1alpha1 "github.com/markfisher/rokn/pkg/internal/thirdparty/keda/v1alpha1"
	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/pkg/kmeta"
)

type DispatcherScaledObjectArgs struct {
	DispatcherDeployment *v1.Deployment
	QueueName            string
	Trigger              *v1beta1.Trigger
	BrokerUrlSecretKey   string
}

type RabbitTriggerAuthenticationArgs struct {
	Namespace  string
	SecretName string
	SecretKey  string
}

const TriggerAuthenticationName = "rabbit-trigger-authentication"

// TODO: move this to the Broker reconciler
func MakeRabbitTriggerAuthentication(args *RabbitTriggerAuthenticationArgs) *kedav1alpha1.TriggerAuthentication {
	return &kedav1alpha1.TriggerAuthentication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: args.Namespace,
			Name:      TriggerAuthenticationName,
		},
		Spec: kedav1alpha1.TriggerAuthenticationSpec{
			SecretTargetRef: []kedav1alpha1.AuthSecretTargetRef{
				{
					Parameter: "host",
					Name:      args.SecretName,
					Key:       args.SecretKey,
				},
			},
			Env: []kedav1alpha1.AuthEnvironment{},
		},
	}
}

func MakeDispatcherScaledObject(args *DispatcherScaledObjectArgs) *kedav1alpha1.ScaledObject {
	zero := int32(0)
	one := int32(1)
	five := int32(5)
	thirty := int32(30)

	deployment := args.DispatcherDeployment
	return &kedav1alpha1.ScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s", deployment.Name),
			Namespace: deployment.Namespace,
			Labels:    deployment.Labels,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(args.Trigger),
			},
		},
		Spec: kedav1alpha1.ScaledObjectSpec{
			ScaleTargetRef: &kedav1alpha1.ObjectReference{
				DeploymentName: deployment.Name,
			},
			PollingInterval: &five,   // seconds
			CooldownPeriod:  &thirty, // seconds
			MinReplicaCount: &zero,
			MaxReplicaCount: &one, // for now
			Triggers: []kedav1alpha1.ScaleTriggers{
				{
					Type: "rabbitmq",
					Metadata: map[string]string{
						"queueName":   args.QueueName,
						"host":        args.BrokerUrlSecretKey,
						"queueLength": "1",
					},
					AuthenticationRef: &kedav1alpha1.ScaledObjectAuthRef{
						Name: TriggerAuthenticationName,
					},
				},
			},
		},
	}
}
