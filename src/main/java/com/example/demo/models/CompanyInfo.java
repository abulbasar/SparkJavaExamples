package com.example.demo.models;

import lombok.Data;

import java.io.Serializable;

@Data
public class CompanyInfo implements Serializable {
    private String symbol;
    private String security;
    private String secFilings;
    private String gisSector;
    private String location;
}
