package com.vivek.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vivek.entity.Product;
import com.vivek.repository.ProductRepository;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;


@Service
public class ProductService {

    @Autowired
    private ProductRepository repository;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper objectMapper = new ObjectMapper();
    @Value("${product.discount.update.topic}")
    private  String topicName;

    public String resetRecords() {
        repository.findAll()
                .forEach(product -> {
                    product.setOfferApplied(false);
                    product.setPriceAfterDiscount(product.getPrice());
                    product.setDiscountPercentage(0);
                    repository.save(product);
                });
        return "Data Reset to DB";
    }

    @Transactional
    public void processProductIds(List<Long> productIds) {
        productIds.parallelStream()
                .forEach(this::fetchUpdateAndPublish);
    }

    private void fetchUpdateAndPublish(Long productId) {

        //fetch product by id
        Product product = repository.findById(productId)
                .orElseThrow(() ->
                        new IllegalArgumentException("Product ID does not exist in the system")
                );

        //update discount properties
        updateDiscountedPrice(product);

        //save to DB
        repository.save(product);

        //kafka events
        publishProductEvent(product);
    }

    private void updateDiscountedPrice(Product product) {

        double price = product.getPrice();

        int discountPercentage = (price >= 1000) ? 10 : (price > 500 ? 5 : 0);

        double priceAfterDiscount = price - (price * discountPercentage / 100);

        if (discountPercentage > 0) {
            product.setOfferApplied(true);
        }
        product.setDiscountPercentage(discountPercentage);
        product.setPriceAfterDiscount(priceAfterDiscount);
    }
    private void publishProductEvent(Product product) {
        try {
            String productJson = objectMapper.writeValueAsString(product);
            kafkaTemplate.send(topicName, productJson);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert product to JSON", e);
        }
    }

    public List<Long> getProductIds(){
        return repository.findAll()
                .stream()
                .map(Product::getId)
                .collect(Collectors.toList());
    }
}
