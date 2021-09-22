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

        // Init 2 consumers for consuming messages from topic A and B
        consumerX = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub-x").subscribe();
        consumerY = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub-y").subscribe();
        consumerZ = client.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub-z").subscribe();

        // Receive message from topic A and then publish it to topic B
        // receive message from topic B and then publish to topic C
        processMessagesFromTopic(consumerX);
        processMessagesFromTopic(consumerY);
        processMessagesFromTopic(consumerZ);
    }

    @RequestMapping("/data-generate")
    @ResponseBody
    public String dataGenerate() throws Exception {
        MessageId messageId = producer.newMessage().value("New Message!").send();
        return messageId.toString();
    }

    private void processMessagesFromTopic(Consumer<String> consumer) {
        new Thread(() -> {
            while (true) {
                try {
                    Message<String> received = consumer.receive();
                    consumer.acknowledge(received);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}
