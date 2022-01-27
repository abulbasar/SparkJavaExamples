package com.example.models;

import lombok.Data;

import java.io.Serializable;


@Data
public class Stock implements Serializable {
    private java.sql.Date date;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Double volume;
    private Double adjclose;
    private String symbol;

    public Stock(){

    }


//    static final byte[] infoColumnFamily = Bytes.toBytes("info");
//    static final byte[] dateCol = Bytes.toBytes("date");
//    static final byte[] openCol = Bytes.toBytes("open");
//    static final byte[] highCol = Bytes.toBytes("high");
//    static final byte[] lowCol = Bytes.toBytes("low");
//    static final byte[] closeCol = Bytes.toBytes("close");
//    static final byte[] volumeCol = Bytes.toBytes("volume");
//    static final byte[] adjcloseCol = Bytes.toBytes("adjclose");
//    static final byte[] symbolCol = Bytes.toBytes("symbol");
//
//
//
//
//
//    public Put toPut(){
//
//        long epoch = date.getTime();
//
//        Put put = new Put(Bytes.toBytes(String.format("%s-%d", symbol, epoch)));
//        if(open != null) {
//            put.addColumn(infoColumnFamily, openCol, Bytes.toBytes(open));
//        }
//        if(high != null) {
//            put.addColumn(infoColumnFamily, highCol, Bytes.toBytes(high));
//        }
//        if(low != null) {
//            put.addColumn(infoColumnFamily, lowCol, Bytes.toBytes(low));
//        }
//        if(close != null) {
//            put.addColumn(infoColumnFamily, closeCol, Bytes.toBytes(close));
//        }
//        if(adjclose != null) {
//            put.addColumn(infoColumnFamily, adjcloseCol, Bytes.toBytes(adjclose));
//        }
//        if(volume != null) {
//            put.addColumn(infoColumnFamily, volumeCol, Bytes.toBytes(volume));
//        }
//        if(symbol != null) {
//            put.addColumn(infoColumnFamily, symbolCol, Bytes.toBytes(symbol));
//        }
//        if(date != null) {
//            put.addColumn(infoColumnFamily, dateCol, Bytes.toBytes(epoch));
//        }
//
//        return put;
//    }
//
//    public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> toKVPairs(){
//        long epoch = date.getTime();
//        byte[] rowkey = Bytes.toBytes(String.format("%s-%d", symbol, epoch));
//        List<KeyValue> keyValues = new ArrayList<>();
//
//        keyValues.add(new KeyValue(rowkey, infoColumnFamily, openCol, Bytes.toBytes(open)));
//        keyValues.add(new KeyValue(rowkey, infoColumnFamily, closeCol, Bytes.toBytes(close)));
//        keyValues.add(new KeyValue(rowkey, infoColumnFamily, highCol, Bytes.toBytes(high)));
//        keyValues.add(new KeyValue(rowkey, infoColumnFamily, lowCol, Bytes.toBytes(low)));
//        keyValues.add(new KeyValue(rowkey, infoColumnFamily, adjcloseCol, Bytes.toBytes(adjclose)));
//        keyValues.add(new KeyValue(rowkey, infoColumnFamily, dateCol, Bytes.toBytes(epoch)));
//        keyValues.add(new KeyValue(rowkey, infoColumnFamily, symbolCol, Bytes.toBytes(symbol)));
//
//        return keyValues.stream().map(r -> new Tuple2<>(new ImmutableBytesWritable(rowkey), r)).iterator();
//    }
//
//    public static Stock parse(Result result){
//        Stock stock = new Stock();
//        stock.setOpen(Bytes.toDouble(result.getValue(infoColumnFamily, openCol)));
//        stock.setClose(Bytes.toDouble(result.getValue(infoColumnFamily, closeCol)));
//        stock.setHigh(Bytes.toDouble(result.getValue(infoColumnFamily, highCol)));
//        stock.setLow(Bytes.toDouble(result.getValue(infoColumnFamily, lowCol)));
//        stock.setAdjclose(Bytes.toDouble(result.getValue(infoColumnFamily, adjcloseCol)));
//        stock.setSymbol(Bytes.toString(result.getValue(infoColumnFamily, symbolCol)));
//
//        long epoch = Bytes.toLong(result.getValue(infoColumnFamily, dateCol));
//
//        Date date = new java.sql.Date(epoch);
//        stock.setDate(date);
//
//        return stock;
//
//    }
//
//    public ImmutableBytesWritable toKey(){
//        long epoch = date.getTime();
//        byte[] rowkey = Bytes.toBytes(String.format("%s-%d", symbol, epoch));
//        return new ImmutableBytesWritable(rowkey);
//    }





}
