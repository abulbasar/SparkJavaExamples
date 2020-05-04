package com.example.demo.models;


import lombok.Data;

import java.io.Serializable;
import java.time.LocalDate;

@Data
public class Stock implements Serializable {

    private LocalDate date;
    private Double close;
    private Double open;
    private Double high;
    private Double low;
    private Double volume;
    private String symbol;


}
