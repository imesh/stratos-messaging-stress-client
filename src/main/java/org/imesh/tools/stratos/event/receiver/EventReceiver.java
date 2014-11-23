package org.imesh.tools.stratos.event.receiver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.stratos.messaging.broker.subscribe.Subscriber;
import org.apache.stratos.messaging.message.JsonMessage;
import org.imesh.tools.stratos.event.TextMessageEvent;

import javax.security.auth.DestroyFailedException;
import javax.security.auth.Destroyable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Event receiver
 */
public class EventReceiver implements Runnable {
    private static final Log log = LogFactory.getLog(EventReceiver.class);

    private String id;
    private String topicName;
    private List<String> messageList;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);
    private Subscriber topicSubscriber;

    public EventReceiver(String id, String topicName) {
        this.id = id;
        this.topicName = topicName;
        this.messageList = new ArrayList<String>();
    }

    @Override
    public void run() {
        topicSubscriber = new Subscriber(topicName, new org.apache.stratos.messaging.broker.subscribe.MessageListener() {

            @Override
            public void messageReceived(org.apache.stratos.messaging.domain.Message message) {
                try {
                    String messageText = new String(message.getText());
                    TextMessageEvent event = (TextMessageEvent) (new JsonMessage(messageText, TextMessageEvent.class)).getObject();
                    messageList.add(event.getMessage());
                    log.debug("Message received: " + event.getMessage());
                } catch (Exception e) {
                    log.error(e);
                }
            }
        });
        topicSubscriber.run();

        if (log.isDebugEnabled()) {
            log.debug("Topology event message receiver thread started");
        }
    }

    public List<String> getMessageList() {
        return messageList;
    }

    public String getId() {
        return id;
    }

    public void terminate() {
        topicSubscriber.terminate();
    }
}
