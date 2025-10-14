package com.example.microservice_problems.enums;

public enum DbEnum {
    DB_COSTUMER("ms_customer"),
    DB_SALE("ms_sale");

    private final String name;
    DbEnum(String name) {this.name = name;}
    public String getName() {return name;}
}
