package com.example.microservice_problems.repositories;

import com.example.microservice_problems.models.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Long> {
}
