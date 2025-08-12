package com.kafkaavroschema.repository;

import com.kafkaavroschema.dto.Employee;
import com.kafkaavroschema.entity.EmployeeEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EmployeeRepository extends JpaRepository<EmployeeEntity, Long> {}

