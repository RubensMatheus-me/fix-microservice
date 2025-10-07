package com.example.microservice_problems.config;


import com.example.microservice_problems.enums.DbEnum;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DynamicJdbcTemplateProvider {

    @Value("${spring.datasource.baseurl}")
    private String baseUrl;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;

    private final Map<String, JdbcTemplate> templates = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        for (DbEnum db : DbEnum.values()) {
            createJdbcTemplate(db.getName());
        }
    }

    public JdbcTemplate getJdbcTemplate(String databaseName) {
        return templates.computeIfAbsent(databaseName, this::createJdbcTemplate);
    }

    private JdbcTemplate createJdbcTemplate(String databaseName) {
        DataSource ds = DataSourceBuilder.create()
                .url(baseUrl + databaseName)
                .username(username)
                .password(password)
                .driverClassName("org.postgresql.Driver")
                .build();

        return new JdbcTemplate(ds);
    }

}