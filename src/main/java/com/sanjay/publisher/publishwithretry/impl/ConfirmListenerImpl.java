package com.sanjay.publisher.publishwithretry.impl;

import com.rabbitmq.client.ConfirmListener;
import com.sanjay.publisher.publishwithretry.interfaces.RetryPublishService;
import lombok.AllArgsConstructor;

import java.io.IOException;

@AllArgsConstructor
public class ConfirmListenerImpl implements ConfirmListener {

    private final RetryPublishService retryPublishService;

    @Override
    public void handleAck(long deliveryTag, boolean multiple) throws IOException {
        System.out.println("Message published successfully. DeliveryTag: " + deliveryTag);
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple) throws IOException {
        // Retry publishing the message
        retryPublishService.retryPublish(deliveryTag);
    }
}
