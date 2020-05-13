/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resources

import (
	"fmt"

	"github.com/streadway/amqp"
	eventingv1alpha1 "knative.dev/eventing/pkg/apis/eventing/v1alpha1"
)

// BindingArgs are the arguments to create a Trigger's Binding to a RabbitMQ Exchange.
type BindingArgs struct {
	Trigger     *eventingv1alpha1.Trigger
	RoutingKey  string
	QueueName   string
	RabbitmqURL string
}

// MakeBinding declares the Binding from the Broker's Exchange to the Trigger's Queue.
func MakeBinding(args *BindingArgs) error {
	conn, err := amqp.Dial(args.RabbitmqURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	table := amqp.Table{
		"x-match": "all",
	}
	for key, val := range *args.Trigger.Spec.Filter.Attributes {
		table[key] = val
	}

	exchangeName := fmt.Sprintf("%s/%s", args.Trigger.Namespace, ExchangeName(args.Trigger.Spec.Broker))
	queueName := fmt.Sprintf("%s/%s", args.Trigger.Namespace, args.Trigger.Name)
	nowait := false
	err = channel.QueueBind(queueName, args.RoutingKey, exchangeName, nowait, table)
	if err != nil {
		return err
	}
	return nil
}

// ExchangeName derives the Exchange name from the Broker name
func ExchangeName(brokerName string) string {
	return fmt.Sprintf("knative-%s", brokerName)
}
