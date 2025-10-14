package com.example.microservice_problems.external_services.kafka.event;

import com.example.microservice_problems.enums.DbEnum;

public record ReplicationRetryEvent(
        DbEnum targetDb,
        DebeziumEvent event
) {}