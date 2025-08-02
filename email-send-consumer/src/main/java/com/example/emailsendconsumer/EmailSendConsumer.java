package com.example.emailsendconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EmailSendConsumer {
    @KafkaListener(
            topics = "email.send",
            groupId = "email-send-group" // 컨슈머 그룹 이름
    )
    public void consume(String message) {
        System.out.println("Kafka로부터 받아온 메시지: " + message);

        EmailSendMessage emailSendMessage = EmailSendMessage.fromJson(message);

        // 실제 이메일 발송 로직 부분
        try {
            Thread.sleep(3000); // 가정: 이메일 발송 3초
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("이메일 발송 완료");
    }
}
