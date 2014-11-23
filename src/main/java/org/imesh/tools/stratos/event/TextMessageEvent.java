package org.imesh.tools.stratos.event;

import org.apache.stratos.messaging.event.Event;

/**
 * Created by imesh on 11/7/14.
 */
public class TextMessageEvent extends Event {
    private String message;

    public TextMessageEvent(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
