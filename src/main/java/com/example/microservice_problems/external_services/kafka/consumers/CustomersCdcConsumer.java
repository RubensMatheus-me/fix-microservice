package com.example.microservice_problems.external_services.kafka.consumers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;

@Component
@RequiredArgsConstructor
public class CustomersCdcConsumer {

    private final ObjectMapper om = new ObjectMapper();
    private final JdbcTemplate jdbc;

    @KafkaListener(topics = "pg.public.customers", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onMessage(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        try {
            JsonNode root = om.readTree(rec.value());
            String op = root.path("op").asText();
            JsonNode after = root.path("after");
            JsonNode before = root.path("before");

            switch (op) {
                case "c":
                case "u":
                case "r": {
                    long id = after.get("id").asLong();
                    String name = after.get("name").asText();
                    String email = after.get("email").asText();
                    OffsetDateTime createdAt = OffsetDateTime.parse(after.get("created_at").asText());
                    OffsetDateTime updatedAt = OffsetDateTime.parse(after.get("updated_at").asText());

                    jdbc.update("""
                      INSERT INTO ms_customer.customers (id, name, email, created_at, updated_at)
                      VALUES (?, ?, ?, ?, ?)
                      ON CONFLICT (id) DO UPDATE
                        SET name = EXCLUDED.name,
                            email = EXCLUDED.email,
                            created_at = EXCLUDED.created_at,
                            updated_at = EXCLUDED.updated_at
                    """, id, name, email, createdAt, updatedAt);
                                        break;
                }
                case "d": {
                    long id;
                    if (before != null && before.hasNonNull("id")) {
                        id = before.get("id").asLong();
                    } else {
                        id = om.readTree(rec.key()).get("id").asLong();
                    }
                    jdbc.update("DELETE FROM ms_customer.customers WHERE id = ?", id);

                    break;
                }
                default:
            }

            ack.acknowledge();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
