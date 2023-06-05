package com.sanjay.publisher.publishwithretry.interfaces;

public interface MessageService {

    String getMessage(long id);

    void setMessage(long id, String message);
}
