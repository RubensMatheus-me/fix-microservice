package com.example.microservice_problems.enums;

public enum DbEnum {
    DB1("db1"),
    DB2("db2");

    private final String name;
    DbEnum(String name) {this.name = name;}
    public String getName() {return name;}
}
