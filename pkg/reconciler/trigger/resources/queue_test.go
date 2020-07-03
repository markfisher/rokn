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

const namespace = "foobar"
const triggerName = "my-queue"

func TestQueueDeclaration(t *testing.T) {
	ctx := context.Background()
	rabbitContainer := testrabbit.AutoStartRabbit(t, ctx)
	defer testrabbit.TerminateContainer(t, ctx, rabbitContainer)

	queue, err := resources.DeclareQueue(&resources.QueueArgs{
		Trigger: &eventingv1beta1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      triggerName,
				Namespace: namespace,
			},
		},
		RabbitmqURL: testrabbit.BrokerUrl(t, ctx, rabbitContainer),
	})

	assert.NilError(t, err)
	assert.Equal(t, queue.Name, fmt.Sprintf("%s/%s", namespace, triggerName))
}

func TestIncompatibleQueueDeclarationFailure(t *testing.T) {
	ctx := context.Background()
	rabbitContainer := testrabbit.AutoStartRabbit(t, ctx)
	defer testrabbit.TerminateContainer(t, ctx, rabbitContainer)
	queueName := fmt.Sprintf("%s/%s", namespace, triggerName)
	testrabbit.CreateNonDurableQueue(t, ctx, rabbitContainer, queueName)

	_, err := resources.DeclareQueue(&resources.QueueArgs{
		Trigger: &eventingv1beta1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      triggerName,
				Namespace: namespace,
			},
		},
		RabbitmqURL: testrabbit.BrokerUrl(t, ctx, rabbitContainer),
	})

	assert.ErrorContains(t, err, fmt.Sprintf("inequivalent arg 'durable' for queue '%s'", queueName))
}

func TestQueueDeletion(t *testing.T) {
	ctx := context.Background()
	rabbitContainer := testrabbit.AutoStartRabbit(t, ctx)
	defer testrabbit.TerminateContainer(t, ctx, rabbitContainer)
	queueName := fmt.Sprintf("%s/%s", namespace, triggerName)
	testrabbit.CreateDurableQueue(t, ctx, rabbitContainer, queueName)

	err := resources.DeleteQueue(&resources.QueueArgs{
		Trigger: &eventingv1beta1.Trigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      triggerName,
				Namespace: namespace,
			},
		},
		RabbitmqURL: testrabbit.BrokerUrl(t, ctx, rabbitContainer),
	})

	assert.NilError(t, err)
	queues := testrabbit.FindQueues(t, ctx, rabbitContainer)
	assert.Equal(t, len(queues), 0)
}
