package com.example.emailsendconsumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EmailSendMessage {
    private String from;         // 발신자 이메일
    private String to;           // 수신자 이메일
    private String subject;      // 이메일 제목
    private String body;         // 이메일 본문

    // 역직렬화(String 형태의 카프카 메시지 -> Java 객체)시 필요함
    public EmailSendMessage() {
    }

    public EmailSendMessage(String from, String to, String subject, String body) {
        this.from = from;
        this.to = to;
        this.subject = subject;
        this.body = body;
    }

    // Json 값을 EmailSendMessage로 역직렬화하는 메서드
    public static EmailSendMessage fromJson(String json) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(json, EmailSendMessage.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON 파싱 실패");
        }
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public String getSubject() {
        return subject;
    }

    public String getBody() {
        return body;
    }
}
