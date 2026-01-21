package com.gabriel.pss.kafka;

import com.gabriel.pss.payload.Order;
import com.gabriel.pss.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaConsumer.class);

    private OrderRepository orderRepository;

    public JsonKafkaConsumer(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @KafkaListener(topics = "pss_order", groupId = "order-consumer-group")
    public void consume(Order order) {
        LOGGER.info(String.format("Json message received -> %s", order.toString()));
        Order savedOrder = orderRepository.save(order);
        LOGGER.info(String.format("Order saved to database with ID: %d", savedOrder.getId()));
    }
}
