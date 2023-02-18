package com.svj.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.svj.dto.PaymentRequest;
import com.svj.dto.PaytmRequest;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Random;
import java.util.UUID;

@RestController
public class PaytmController {
    private KafkaTemplate<String, Object> kafkaTemplate;
    private ObjectMapper objectMapper= new ObjectMapper();

    @Value("${paytm.producer.topic.name}")
    private String topicName;

    public PaytmController(KafkaTemplate kafkaTemplate){
        this.kafkaTemplate= kafkaTemplate;
    }

//    @GetMapping("/publish/{message}")
//    public void sendMessage(@PathVariable String message){
//        for(int i=0; i<10000; i++)
//            kafkaTemplate.send(topicName, message+i);
//    }

    @PostConstruct
    public void setup(){
        objectMapper.registerModule(new JavaTimeModule());
    }
    @PostMapping("/paytm/payment")
    public String doPayment(@RequestBody PaytmRequest<PaymentRequest> paytmRequest) throws JsonProcessingException {
        for(int i=1; i<= 100000; i++){
            PaymentRequest paymentRequest= paytmRequest.getPayload();
            paymentRequest.setSourceAccount("SRC_AC"+i);
            paymentRequest.setDestnAccount("DSN_AC"+i);
            paymentRequest.setAmount(new Random().nextInt(10000));
            paymentRequest.setTransactionId(UUID.randomUUID().toString());
            paymentRequest.setTxDate(LocalDate.now());
            kafkaTemplate.send(topicName, paymentRequest);
        }
        return "payment instanitated sucessfully...";
    }

}
