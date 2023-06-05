package com.sanjay.publisher.publishwithretry.interfaces;

import com.rabbitmq.client.Channel;

import java.io.IOException;

public interface RetryPublishService {

    void retryPublish(long deliveryTag) throws IOException;
}
