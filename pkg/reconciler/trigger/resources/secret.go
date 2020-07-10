package resources

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"knative.dev/pkg/kmeta"
)

const (
	ScaleTriggerSecretName   = "rabbitmq-scale-trigger-secret"
	RabbitMqBrokerUrlKeyName = "RabbitMqBrokerUrl"
)

type TriggerSecretArgs struct {
	BrokerUrl string
	Trigger   *v1beta1.Trigger
}

func MakeTriggerSecret(args *TriggerSecretArgs) *corev1.Secret {
	trigger := args.Trigger

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ScaleTriggerSecretName,
			Namespace: trigger.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*kmeta.NewControllerRef(trigger),
			},
		},
		Data: map[string][]byte{
			RabbitMqBrokerUrlKeyName: []byte(args.BrokerUrl),
		},
	}
}
