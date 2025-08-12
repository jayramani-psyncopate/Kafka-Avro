package com.kafkaavroschema.controller;

import com.kafkaavroschema.dto.Employee;
import com.kafkaavroschema.entity.EmployeeEntity;
import com.kafkaavroschema.producer.KafkaAvroProducer;
import com.kafkaavroschema.repository.EmployeeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.time.ZoneId;

@RestController
public class KafkaAvroController {

    @Autowired
    private KafkaAvroProducer kafkaProducer;

    @Autowired
    private EmployeeRepository employeeRepository;

    @PostMapping("/send")
    public ResponseEntity<EmployeeEntity> create(@RequestBody Employee req) {
        EmployeeEntity e = new EmployeeEntity();
        if (req.getName() != null) e.setName(req.getName().toString());
        if (req.getDepartment() != null) e.setDepartment(req.getDepartment().toString());
        EmployeeEntity saved = employeeRepository.save(e);

        Employee resp = Employee.newBuilder()
                .setId(saved.getId())
                .setName(saved.getName())
                .setDepartment(saved.getDepartment())
                .setUpdatedAt(
                saved.getUpdatedAt() != null
                        ? saved.getUpdatedAt().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                        : System.currentTimeMillis()
        )
                .build();

        kafkaProducer.send(resp);

        return ResponseEntity.ok(saved);
    }
}
