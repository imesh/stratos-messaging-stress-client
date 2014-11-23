/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.imesh.tools.stratos.event;

import org.apache.log4j.PropertyConfigurator;
import org.apache.stratos.messaging.broker.publish.EventPublisher;
import org.apache.stratos.messaging.broker.publish.EventPublisherPool;
import org.apache.stratos.messaging.domain.topology.*;
import org.apache.stratos.messaging.event.Event;
import org.apache.stratos.messaging.event.topology.*;
import org.imesh.tools.stratos.event.generator.TopologyEventGenerator;
import org.imesh.tools.stratos.event.generator.TopologyUtil;
import org.imesh.tools.stratos.event.receiver.EventReceiver;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Run this main class to send a set of sample topology events.
 */
public class Main {

    private static final int MESSAGE_COUNT = 1000;
    private static final int INTERVAL_BETWEEN_MESSAGES = 1;
    private static final int SUBSCRIBER_COUNT = 10;

    public static void main(String[] args) {
        // Configure log4j properties
        PropertyConfigurator.configure(System.getProperty("log4j.properties.file.path", "src/main/conf/log4j.properties"));
        System.setProperty("jndi.properties.dir", System.getProperty("jndi.properties.dir", "src/main/conf"));

        ExecutorService executorService = Executors.newFixedThreadPool(SUBSCRIBER_COUNT);
        List<EventReceiver> receiverList = new ArrayList<EventReceiver>();

        for (int i = 1; i < SUBSCRIBER_COUNT; i++) {
            EventReceiver receiver = new EventReceiver(String.valueOf(i), "text");
            receiverList.add(receiver);

            executorService.submit((Runnable) receiver);
        }

        EventPublisher publisher = EventPublisherPool.getPublisher("text");
        List<TextMessageEvent> eventList = new ArrayList<TextMessageEvent>();
        System.out.println("Publishing messages: " + MESSAGE_COUNT);

        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            TextMessageEvent event = new TextMessageEvent("hello-world-" + i);
            eventList.add(event);
            publisher.publish(event);
            try {
                Thread.sleep(INTERVAL_BETWEEN_MESSAGES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (EventReceiver receiver : receiverList) {
            for (TextMessageEvent event : eventList) {
                if (!eventExistInReceiver(event, receiver)) {
                    throw new RuntimeException("Subscriber " + receiver.getId() + " has not received message: " + event.getMessage());
                }
            }
            System.out.println("receiver-" + receiver.getId() + ": [messages-received] " + receiver.getMessageList().size());
        }

        for (EventReceiver receiver : receiverList) {
            receiver.terminate();
        }

        System.out.println("Shutting down...");
        executorService.shutdownNow();
    }

    private static boolean eventExistInReceiver(TextMessageEvent event, EventReceiver receiver) {
        for (String message : receiver.getMessageList()) {
            if (message.equals(event.getMessage())) {
                return true;
            }
        }
        return false;
    }
}
