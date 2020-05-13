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

// ExchangeArgs are the arguments to create a RabbitMQ Exchange.
type ExchangeArgs struct {
	Broker      *eventingv1alpha1.Broker
	RabbitmqURL string
}

// DeclareExchange declares the Exchange for a Broker.
func DeclareExchange(args *ExchangeArgs) error {
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

	exchangeName := fmt.Sprintf("%s/%s", args.Broker.Namespace, ExchangeName(args.Broker.Name))
	err = channel.ExchangeDeclare(
		exchangeName,
		"headers", // kind
		true,      // durable
		false,     // auto-delete
		false,     // internal
		false,     // nowait
		nil,       // args
	)
	if err != nil {
		return err
	}

	return nil
}

// DeleteExchange deletes the Exchange for a Broker.
func DeleteExchange(args *ExchangeArgs) error {
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

	exchangeName := fmt.Sprintf("%s/%s", args.Broker.Namespace, ExchangeName(args.Broker.Name))
	err = channel.ExchangeDelete(
		exchangeName,
		false, // if-unused
		false, // nowait
	)
	if err != nil {
		return err
	}

	return nil
}

// ExchangeName derives the Exchange name from the Broker name
func ExchangeName(brokerName string) string {
	return fmt.Sprintf("knative-%s", brokerName)
}
