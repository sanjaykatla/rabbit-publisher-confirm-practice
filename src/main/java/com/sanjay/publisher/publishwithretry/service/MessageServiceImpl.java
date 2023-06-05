package com.sanjay.publisher.publishwithretry.service;


import com.sanjay.publisher.publishwithretry.interfaces.MessageService;

import java.util.HashMap;
import java.util.Map;

public class MessageServiceImpl implements MessageService {

    private static Map<Long, String> deliveryTagToMessageMap = new HashMap<>();

    @Override
    public String getMessage(long id) {
        return deliveryTagToMessageMap.get(id);
    }

    @Override
    public void setMessage(long id, String message) {
        deliveryTagToMessageMap.put(id, message);
    }
}
