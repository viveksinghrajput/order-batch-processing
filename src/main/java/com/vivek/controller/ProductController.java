package com.vivek.controller;

import com.vivek.service.ProductService;
import com.vivek.service.ProductServiceV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/products")
public class ProductController {

    @Autowired
    private ProductService productService;
    @Autowired
    private ProductServiceV2 productServiceV2;


    //this endpoint for testing
    @GetMapping("/ids")
    public ResponseEntity<List<Long>> getIds() {
        return ResponseEntity.ok(productService.getProductIds());
    }

    //this endpoint for data reset
    @PostMapping("/reset")
    public ResponseEntity<String> resetProductRecords() {
        String response = productService.resetRecords();
        return ResponseEntity.ok(response);
    }

    @PostMapping("/process")
    public ResponseEntity<String> processProductIds(@RequestBody List<Long> productIds) {
        productService.processProductIds(productIds);
        return ResponseEntity.ok("Products processed and events published.");
    }

    @PostMapping("/process/v2")
    public ResponseEntity<String> processProductIdsV2(@RequestBody List<Long> productIds) {
        productServiceV2.executeProductIds(productIds);
        return ResponseEntity.ok("Products processed and events published.");
    }
}
