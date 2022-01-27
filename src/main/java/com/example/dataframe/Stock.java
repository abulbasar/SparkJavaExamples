package com.example.dataframe;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;

@Data
public class Stock implements Serializable {
    private Timestamp date;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Double volume;
    private Double adjclose;
    private String symbol;
}
