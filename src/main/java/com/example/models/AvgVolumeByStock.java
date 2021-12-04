package com.example.models;


import lombok.Data;

@Data
public class AvgVolumeByStock {
    private String symbol;
    private Double volume;
}
