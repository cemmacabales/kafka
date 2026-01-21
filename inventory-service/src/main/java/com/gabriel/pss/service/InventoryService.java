package com.gabriel.pss.service;

import com.gabriel.pss.model.Inventory;
import com.gabriel.pss.payload.Item;
import com.gabriel.pss.payload.Order;
import com.gabriel.pss.repository.InventoryRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Optional;

@Service
public class InventoryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryRepository inventoryRepository;

    public InventoryService(InventoryRepository inventoryRepository) {
        this.inventoryRepository = inventoryRepository;
    }

    @PostConstruct
    public void init() {
        // Initialize some inventory data
        inventoryRepository.save(new Inventory(1, "Product 1", 100));
        inventoryRepository.save(new Inventory(2, "Product 2", 50));
        inventoryRepository.save(new Inventory(3, "Product 3", 200));
        // Add items from the sample request
        inventoryRepository.save(new Inventory(101, "Laptop", 10));
        inventoryRepository.save(new Inventory(102, "Mouse", 20));
        LOGGER.info("Initialized inventory data");
    }

    @KafkaListener(topics = "pss_order", groupId = "inventory-consumer-group")
    public void consume(Order order) {
        LOGGER.info(String.format("Inventory Service: Order received -> %s", order.toString()));

        if (order.getItems() != null) {
            for (Item item : order.getItems()) {
                updateInventory(item);
            }
        }
    }

    private void updateInventory(Item item) {
        Optional<Inventory> inventoryOptional = inventoryRepository.findById(item.getProductId());
        if (inventoryOptional.isPresent()) {
            Inventory inventory = inventoryOptional.get();
            int newQuantity = inventory.getQuantity() - item.getQuantity();
            if (newQuantity >= 0) {
                inventory.setQuantity(newQuantity);
                inventoryRepository.save(inventory);
                LOGGER.info("Updated inventory for Product ID: " + item.getProductId() + ", New Quantity: " + newQuantity);
            } else {
                LOGGER.warn("Insufficient stock for Product ID: " + item.getProductId());
                // Handle insufficient stock scenario (e.g., send notification, compensate transaction)
            }
        } else {
            LOGGER.warn("Product not found in inventory with ID: " + item.getProductId());
        }
    }
}
