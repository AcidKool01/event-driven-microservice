package com.project.stockservice.kafka;

import com.project.basedomains.dto.OrderEvent;
import com.project.stockservice.entity.StockData;
import com.project.stockservice.repository.StockDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {

    private StockDataRepository dataRepository;

    public OrderConsumer(StockDataRepository dataRepository) {
        this.dataRepository = dataRepository;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener( topics = "${spring.kafka.topic.name}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(OrderEvent event) {
        LOGGER.info(String.format("Order Event received in Stock Service => %s", event.toString()));

        // save the order event in the database

        StockData stockData = new StockData();
        stockData.setStockEventData(event.toString());

        dataRepository.save(stockData);

    }
}
