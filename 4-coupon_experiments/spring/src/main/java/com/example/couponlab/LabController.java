package com.example.couponlab;

import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import static com.example.couponlab.LabModel.*;

@RestController
@RequestMapping("/lab")
class LabController {
    private final LabService service;
    LabController(LabService service) { this.service=service; }
    @GetMapping("/health") Map<String,Object> health() { return service.health(); }
    @PostMapping("/runs") ResponseEntity<Map<String,Object>> create(@RequestBody NewRun run) { return ResponseEntity.status(201).body(service.create(run)); }
    @GetMapping("/runs/{id}") Map<String,Object> snapshot(@PathVariable String id) { return service.snapshot(id); }
    @PostMapping("/runs/{id}/requests") ResponseEntity<Map<String,Object>> submit(@PathVariable String id,@RequestBody NewRequest request) {
        Map<String,Object> response=service.submit(id,request);
        String status=(String)response.get("status");
        if(!terminal(status)) { response.put("stage",status);response.put("status","PENDING"); }
        return ResponseEntity.status("REJECTED".equals(status)?409:"ISSUED".equals(status)?200:202).body(response);
    }
    @GetMapping("/runs/{id}/requests/{request}") Map<String,Object> request(@PathVariable String id,@PathVariable String request) { return service.request(id,request); }
    @PostMapping("/runs/{id}/control") Map<String,Object> control(@PathVariable String id,@RequestBody Control control) { return service.control(id,control); }
    @PostMapping("/runs/{id}/replay") ResponseEntity<Map<String,Object>> replay(@PathVariable String id,@RequestBody RequestRef request) { return ResponseEntity.accepted().body(service.replay(id,request.requestId())); }
    @PostMapping("/runs/{id}/cancel") Map<String,Object> cancel(@PathVariable String id,@RequestBody RequestRef request) { return service.cancel(id,request.requestId()); }
    @ExceptionHandler(LabException.class) ResponseEntity<Map<String,Object>> error(LabException error) { return ResponseEntity.status(error.code).body(Map.of("status",error.code==409?"REJECTED":"ERROR","reason",error.getMessage())); }
    @ExceptionHandler(Exception.class) ResponseEntity<Map<String,Object>> unexpected(Exception error) { return ResponseEntity.status(503).body(Map.of("status","ERROR","reason",KafkaTransport.rootMessage(error))); }
}
