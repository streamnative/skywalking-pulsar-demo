/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.streamnative.demo.controller;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.skywalking.apm.toolkit.trace.Trace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.PostConstruct;

@Controller
@RequestMapping("/demo")
@PropertySource("classpath:application.properties")
public class DataGenerationController {

    private static final Logger log = LoggerFactory.getLogger(DataGenerationController.class);

    @Value("${service.url:pulsar://127.0.0.1:6650}")
    private String serviceUrl;

    private PulsarClient client;
    private Producer<String> producer;
    private Consumer<String> consumerX;
    private Consumer<String> consumerY;
    private Consumer<String> consumerZ;

    @PostConstruct
    public void init() throws Exception {
        final String topic = "demo-topic";
        client = PulsarClient.builder().serviceUrl(serviceUrl).build();
        producer = client.newProducer(Schema.STRING).topic(topic).create();

        // Create 3 consumers for consuming messages from topic
        consumerX = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub-x").subscribe();
        consumerY = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub-y").subscribe();
        consumerZ = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub-z").subscribe();

        new Thread(() -> {
            while (true) {
                processMessageX(consumerX);
            }
        }).start();

        new Thread(() -> {
            while (true) {
                processMessageY(consumerY);
            }
        }).start();

        new Thread(() -> {
            while (true) {
                processMessageZ(consumerZ);
            }
        }).start();
    }

    @RequestMapping("/data-generate")
    @ResponseBody
    public String dataGenerate() throws Exception {
        log.info("Received new data generate request.");
        MessageId messageId = producer.newMessage().value("New Message!").send();
        log.info("Published new message to the topic with message Id {}", messageId);
        return messageId.toString();
    }

    @Trace(operationName = "Application X")
    private void processMessageX(Consumer<String> consumer) {
        try {
            Message<String> received = consumer.receive();
            log.info("[{}] Received new message {} from topic.", consumer.getSubscription(),
                    received.getValue());
            consumer.acknowledge(received);
        } catch (PulsarClientException e) {
            log.error("Failed to process message for subscription {}", consumer.getSubscription());
        }
    }

    @Trace(operationName = "Application Y")
    private void processMessageY(Consumer<String> consumer) {
        try {
            Message<String> received = consumer.receive();
            log.info("[{}] Received new message {} from topic.", consumer.getSubscription(),
                    received.getValue());
            consumer.acknowledge(received);
        } catch (PulsarClientException e) {
            log.error("Failed to process message for subscription {}", consumer.getSubscription());
        }
    }

    @Trace(operationName = "Application Z")
    private void processMessageZ(Consumer<String> consumer) {
        try {
            Message<String> received = consumer.receive();
            log.info("[{}] Received new message {} from topic.", consumer.getSubscription(),
                    received.getValue());
            consumer.acknowledge(received);
        } catch (PulsarClientException e) {
            log.error("Failed to process message for subscription {}", consumer.getSubscription());
        }
    }
}
