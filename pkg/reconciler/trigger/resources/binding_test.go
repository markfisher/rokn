package resources_test

import (
	"context"
	"fmt"
	"github.com/markfisher/rokn/pkg/reconciler/internal/testrabbit"
	"github.com/markfisher/rokn/pkg/reconciler/trigger/resources"
	"gotest.tools/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingv1beta1 "knative.dev/eventing/pkg/apis/eventing/v1beta1"
	"testing"
)

func TestBindingDeclaration(t *testing.T) {
	ctx := context.Background()
	rabbitContainer := testrabbit.AutoStartRabbit(t, ctx)
	defer testrabbit.TerminateContainer(t, ctx, rabbitContainer)
	queueName := "queue-and-a"
	qualifiedQueueName := namespace + "/" + queueName
	testrabbit.CreateDurableQueue(t, ctx, rabbitContainer, qualifiedQueueName)
	brokerName := "some-broker"
	exchangeName := namespace + "/" + "knative-" + brokerName
	testrabbit.CreateExchange(t, ctx, rabbitContainer, exchangeName, "headers")

	err := resources.MakeBinding(&resources.BindingArgs{
		RoutingKey:             "some-key",
		RabbitmqHost:           testrabbit.Host(t, ctx, rabbitContainer),
		RabbitmqUsername:       testrabbit.RabbitUsername,
		RabbitmqPassword:       testrabbit.RabbitPassword,
		RabbitmqManagementPort: testrabbit.ManagementPort(t, ctx, rabbitContainer),
		Trigger: &eventingv1beta1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      queueName,
				Namespace: namespace,
			},
			Spec: eventingv1beta1.TriggerSpec{
				Broker: brokerName,
				Filter: &eventingv1beta1.TriggerFilter{
					Attributes: map[string]string{},
				},
			},
		},
	})

	assert.NilError(t, err)
	createdBindings := testrabbit.FindBindings(t, ctx, rabbitContainer)
	assert.Equal(t, len(createdBindings), 2, "Expected 2 bindings: default + requested one")
	defaultBinding := createdBindings[0]
	assert.Equal(t, defaultBinding["source"], "", "Expected binding to default exchange")
	assert.Equal(t, defaultBinding["destination_type"], "queue")
	assert.Equal(t, defaultBinding["destination"], qualifiedQueueName)
	explicitBinding := createdBindings[1]
	assert.Equal(t, explicitBinding["source"], exchangeName)
	assert.Equal(t, explicitBinding["destination_type"], "queue")
	assert.Equal(t, explicitBinding["destination"], qualifiedQueueName)
	assert.Equal(t, asMap(t, explicitBinding["arguments"])[resources.BindingKey], queueName)
}

func TestMissingExchangeBindingDeclarationFailure(t *testing.T) {
	ctx := context.Background()
	rabbitContainer := testrabbit.AutoStartRabbit(t, ctx)
	defer testrabbit.TerminateContainer(t, ctx, rabbitContainer)
	queueName := "queue-te"
	brokerName := "some-broke-herr"

	err := resources.MakeBinding(&resources.BindingArgs{
		RoutingKey:             "some-key",
		RabbitmqHost:           testrabbit.Host(t, ctx, rabbitContainer),
		RabbitmqUsername:       testrabbit.RabbitUsername,
		RabbitmqPassword:       testrabbit.RabbitPassword,
		RabbitmqManagementPort: testrabbit.ManagementPort(t, ctx, rabbitContainer),
		Trigger: &eventingv1beta1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      queueName,
				Namespace: namespace,
			},
			Spec: eventingv1beta1.TriggerSpec{
				Broker: brokerName,
				Filter: &eventingv1beta1.TriggerFilter{
					Attributes: map[string]string{},
				},
			},
		},
	})

	assert.ErrorContains(t, err, "Failed to declare Binding. Expected 201 response, but got: 404.")
	assert.ErrorContains(t, err, fmt.Sprintf("no exchange '%s/knative-%s'", namespace, brokerName))
}

func asMap(t *testing.T, value interface{}) map[string]interface{} {
	result, ok := value.(map[string]interface{})
	assert.Equal(t, ok, true)
	return result
}
