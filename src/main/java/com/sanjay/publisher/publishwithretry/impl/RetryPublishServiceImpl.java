package com.sanjay.publisher.publishwithretry.impl;

import com.rabbitmq.client.Channel;
import com.sanjay.publisher.publishwithretry.interfaces.MessageService;
import com.sanjay.publisher.publishwithretry.interfaces.RetryPublishService;
import lombok.AllArgsConstructor;

import java.io.IOException;

import static com.sanjay.publisher.publishwithretry.interfaces.Constants.QUEUE_NAME;

@AllArgsConstructor
public class RetryPublishServiceImpl implements RetryPublishService {

    int retryCount = 1;
    long retryDelay = 1000; // Initial retry delay in milliseconds
    private final static int MAX_RETRY_ATTEMPTS = 3;
    private final Channel channel;
    private final MessageService messageService;

    @Override
    public void retryPublish(long deliveryTag) throws IOException {
        String message = messageService.getMessage(deliveryTag);
        System.out.println("Message publishing failed. DeliveryTag: " + deliveryTag + ", Message: " + message);

        while (retryCount <= MAX_RETRY_ATTEMPTS) {
            System.out.println("Retry attempt #" + retryCount);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());

            try {
                // Wait for the publisher confirmation
                channel.waitForConfirmsOrDie();
                System.out.println("Message published successfully after retry. DeliveryTag: " + deliveryTag);
                return;
            } catch (InterruptedException e) {
                System.err.println("Message publishing interrupted during retry.");
            }

            // Exponential backoff delay between retries
            try {
                Thread.sleep(retryDelay);
            } catch (InterruptedException e) {
                System.err.println("Sleep interrupted during retry delay.");
            }

            retryCount++;
            retryDelay *= 2; // Double the retry delay for the next attempt
        }

        System.out.println("Reached maximum retry attempts. Message publishing failed. DeliveryTag: " + deliveryTag);
    }
}
