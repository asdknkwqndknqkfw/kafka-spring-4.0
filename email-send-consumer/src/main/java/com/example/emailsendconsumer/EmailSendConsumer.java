package com.example.emailsendconsumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
public class EmailSendConsumer {
    @KafkaListener(
        topics = "email.send",
        groupId = "email-send-group" // 컨슈머 그룹 이름
    )
    @RetryableTopic(
        attempts = "5", // 재시도 총 5회 (재시도 너무 많으면 시스템 부하)
        backoff = @Backoff(delay = 1000, multiplier = 2),    // 1초 간격 재시도, 2배수씩(1초 > 2초 > 4초 ..)
        dltTopicSuffix = ".dlt" // dlt topic명: email.send.dlt
    )
    public void consume(String message) {
        System.out.println("Kafka로부터 받아온 메시지: " + message);

        EmailSendMessage emailSendMessage = EmailSendMessage.fromJson(message);

        if (emailSendMessage.getTo().equals("fail@naver.com")) {
            String errorMessage = "잘못된 이메일 주소로 인해 발송 실패";
            System.out.println(errorMessage);
            throw new RuntimeException(errorMessage);
        }

        // 실제 이메일 발송 로직 부분
        try {
            Thread.sleep(3000); // 가정: 이메일 발송 3초
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("이메일 발송 완료");
    }
}
