package com.example.microservice_problems.external_services.kafka.consumers;

import com.example.microservice_problems.config.DynamicJdbcTemplateProvider;
import com.example.microservice_problems.enums.DbEnum;
import com.example.microservice_problems.external_services.kafka.event.DebeziumEvent;
import com.example.microservice_problems.external_services.kafka.event.ReplicationRetryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class CustomersCdcConsumer {

    private final ObjectMapper om = new ObjectMapper();
    private final DynamicJdbcTemplateProvider jdbcProvider;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final String RETRY_TOPIC = "pg.public.customers.replicate.retry";

    @KafkaListener(topics = "pg.public.customers", containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onMessage(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        try {
            DebeziumEvent event = om.readValue(rec.value(), DebeziumEvent.class);
            String originalEvent = rec.value();

            for (DbEnum db : DbEnum.values()) {
                try {
                    replicateEventToDb(db, event);
                } catch (Exception dbEx) {
                    log.error("Erro ao replicar evento para o banco {}. Enviando para retry...", db.getName(), dbEx);

                    ReplicationRetryEvent retryEvent = new ReplicationRetryEvent(db, event);
                    String retryPayload = om.writeValueAsString(retryEvent);

                    kafkaTemplate.send(RETRY_TOPIC, retryPayload);
                }
            }

            ack.acknowledge();

        } catch (Exception e) {
            throw new RuntimeException("Erro ao processar evento Debezium", e);
        }
    }

    @KafkaListener(topics = "pg.public.customers.replicate.retry", groupId = "customer-replicate-retry-consumer")
    public void onRetryMessage(ConsumerRecord<String, String> rec, Acknowledgment ack) {
        try {
            ReplicationRetryEvent retryEvent = om.readValue(rec.value(), ReplicationRetryEvent.class);
            replicateEventToDb(retryEvent.targetDb(), retryEvent.event());

            ack.acknowledge();

        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public void replicateEventToDb(DbEnum db, DebeziumEvent event) throws SQLException {
        var jdbc = jdbcProvider.getJdbcTemplate(db.getName());
        var databaseString = "public."+event.source().table();
        log.info("DATABASE {}", db.getName());

        Map<String, Object> after = event.after();
        Map<String, Object> before = event.before();
        String op = event.op();

        switch (op) {
            case "c":
            case "u":
            case "r": {
                if (after == null) return;

                long id = ((Number) after.get("id")).longValue();
                String name = (String) after.get("name");
                String email = (String) after.get("email");
                OffsetDateTime createdAt = OffsetDateTime.parse((String) after.get("created_at"));
                OffsetDateTime updatedAt = OffsetDateTime.parse((String) after.get("updated_at"));
                String sql = """
                    INSERT INTO %s (id, name, email, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE
                        SET name = EXCLUDED.name,
                            email = EXCLUDED.email,
                            created_at = EXCLUDED.created_at,
                            updated_at = EXCLUDED.updated_at
                """.formatted(databaseString);
                log.info(sql);
                jdbc.update(sql, id, name, email, createdAt, updatedAt);
                break;
            }
            case "d": {
                long id;
                if (before != null && before.get("id") != null) {
                    id = ((Number) before.get("id")).longValue();
                } else {
                    throw new IllegalArgumentException("Chave primária ausente no evento de delete");
                }

                jdbc.update("DELETE FROM ms_customer.customers WHERE id = ?", id);
                break;
            }
            default:
                log.warn("Operação desconhecida: {}", op);
        }
    }
}
