package com.example.emailsendproducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EmailService {
    // key, value: <String, String>
    private final KafkaTemplate<String, String> kafkaTemplate;

    public EmailService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendEmail(SendEmailRequestDto requestDto) {
        EmailSendMessage emailSendMessage = new EmailSendMessage(
            requestDto.getFrom(),
            requestDto.getTo(),
            requestDto.getSubject(),
            requestDto.getBody()
        );

        this.kafkaTemplate.send("email.send", toJsonString(emailSendMessage));
    }

    // Java Object > String
    // 이유: value-serializer: StringSerializer [application.yml])
    private String toJsonString(Object object) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            String message = objectMapper.writeValueAsString(object);
            return message;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Json 직렬화 실패");
        }
    }
}
