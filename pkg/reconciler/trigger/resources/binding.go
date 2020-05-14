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
	"reflect"

	rabbithole "github.com/michaelklishin/rabbit-hole"
	eventingv1alpha1 "knative.dev/eventing/pkg/apis/eventing/v1alpha1"
)

const (
	managementPort = 15672
	bindingKey     = "x-knative-trigger"
)

// BindingArgs are the arguments to create a Trigger's Binding to a RabbitMQ Exchange.
type BindingArgs struct {
	Trigger          *eventingv1alpha1.Trigger
	RoutingKey       string
	QueueName        string
	RabbitmqHost     string
	RabbitmqUsername string
	RabbitmqPassword string
}

// MakeBinding declares the Binding from the Broker's Exchange to the Trigger's Queue.
func MakeBinding(args *BindingArgs) error {
	adminURL := fmt.Sprintf("http://%s:%d", args.RabbitmqHost, managementPort)
	c, err := rabbithole.NewClient(adminURL, args.RabbitmqUsername, args.RabbitmqPassword)
	if err != nil {
		return fmt.Errorf("Failed to create RabbitMQ Admin Client: %v", err)
	}

	exchangeName := fmt.Sprintf("%s/%s", args.Trigger.Namespace, ExchangeName(args.Trigger.Spec.Broker))
	queueName := fmt.Sprintf("%s/%s", args.Trigger.Namespace, args.Trigger.Name)
	arguments := map[string]interface{}{
		"x-match":  interface{}("all"),
		bindingKey: interface{}(args.Trigger.Name),
	}
	for key, val := range *args.Trigger.Spec.Filter.Attributes {
		arguments[key] = interface{}(val)
	}

	var existing *rabbithole.BindingInfo

	bindings, err := c.ListBindings()
	if err != nil {
		return err
	}

	for _, b := range bindings {
		if val, exists := b.Arguments[bindingKey]; exists && val == args.Trigger.Name {
			existing = &b
			break
		}
	}

	if existing == nil || !reflect.DeepEqual(existing.Arguments, arguments) {
		_, err = c.DeclareBinding("/", rabbithole.BindingInfo{
			Vhost:           "/",
			Source:          exchangeName,
			Destination:     queueName,
			DestinationType: "queue",
			RoutingKey:      args.RoutingKey,
			Arguments:       arguments,
		})
		if err != nil {
			return fmt.Errorf("Failed to declare Binding: %v", err)
		}
		if existing != nil {
			_, err = c.DeleteBinding(existing.Vhost, *existing)
			if err != nil {
				return fmt.Errorf("Failed to delete existing Binding: %v", err)
			}
		}
	}
	return nil
}

// ExchangeName derives the Exchange name from the Broker name
func ExchangeName(brokerName string) string {
	return fmt.Sprintf("knative-%s", brokerName)
}
