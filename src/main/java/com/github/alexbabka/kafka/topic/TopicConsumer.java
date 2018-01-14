package com.github.alexbabka.kafka.topic;

import com.github.alexbabka.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class TopicConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicConsumer.class);

    @Autowired
    private KafkaTemplate<String, String> template;

    @PostConstruct
    private void init() {
        template.send(KafkaConfig.EVENT_TOPIC, "message1");
        template.send(KafkaConfig.EVENT_TOPIC, "message2");
    }

    @KafkaListener(topics = KafkaConfig.EVENT_TOPIC)
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        LOGGER.info("Message received: " + cr.toString());
    }
}
