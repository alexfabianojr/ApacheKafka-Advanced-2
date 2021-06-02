package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailNewOrderService();
        try(var service = new KafkaService<>(EmailNewOrderService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER", emailService::parse, Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, preparing email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        var emailCode = "Thank you for your order! We are processing your order!";
        Order order = record.value().getPayload();
        CorrelationId id = record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispatcher.sendAsync("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailCode);
    }
}
