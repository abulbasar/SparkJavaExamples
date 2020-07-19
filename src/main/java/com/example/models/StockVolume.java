package com.example.models;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class StockVolume implements Serializable {
    private Double sum;
    private Integer count;

    public StockVolume add(StockVolume d2) {
        return  new StockVolume(sum + d2.sum,  count + d2.count);
    }
}
