package com.example.dataframe;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ExcelApp {

    public List<List<Object>> readExcel(String inputFile, int sheetNumber) throws Exception {
        FileInputStream file = new FileInputStream(new File(inputFile));

        //Create Workbook instance holding reference to .xlsx file
        XSSFWorkbook workbook = new XSSFWorkbook(file);

        //Get first/desired sheet from the workbook
        XSSFSheet sheet = workbook.getSheetAt(sheetNumber);

        //Iterate through each rows one by one
        Iterator<Row> rowIterator = sheet.iterator();
        List<List<Object>> rows = new ArrayList<>();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            //For each row, iterate through all the columns
            Iterator<Cell> cellIterator = row.cellIterator();

            final List<Object> cellValues = new ArrayList<>();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                final CellType cellType = cell.getCellType();
                if(CellType.STRING.equals(cell.getCellType())){
                    cellValues.add(cell.getStringCellValue());
                }else if(CellType.NUMERIC.equals(cell.getCellType())){
                    cellValues.add(cell.getNumericCellValue());
                }else if(CellType.BOOLEAN.equals(cell.getCellType())){
                    cellValues.add(cell.getBooleanCellValue());
                }else if(CellType.ERROR.equals(cell.getCellType())){
                    cellValues.add(cell.getErrorCellValue());
                }else if(CellType.FORMULA.equals(cell.getCellType())){
                    cellValues.add(cell.getCellFormula());
                }else{
                    throw new RuntimeException("Invalid type: " + cell.getCellType());
                }
            }
            rows.add(cellValues);
        }
        file.close();
        return rows;
    }

    private SparkSession getSparkSession(){
        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]");
        final SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        System.out.println("Spark UI: " + sparkSession.sparkContext().uiWebUrl());
        return sparkSession;
    }


    public void start(String[] args) throws Exception{
        String inputFile = "/tmp/movies.xlsx";
        final List<List<Object>> rows = readExcel(inputFile, 0);

        final List<org.apache.spark.sql.Row> sparkRows = rows.stream().skip(1)
                .map(cells -> RowFactory.create(cells.toArray(new Object[0])))
                .collect(Collectors.toList());


        final SparkSession sparkSession = getSparkSession();
        StructType schema = new StructType()
                .add("movieId", DataTypes.DoubleType)
                .add("title", DataTypes.StringType)
                .add("genres", DataTypes.StringType)
                ;
        final Dataset<org.apache.spark.sql.Row> dataset = sparkSession.createDataFrame(sparkRows, schema);
        dataset.show();

    }

    public static void main(String[] args) throws Exception{
        final ExcelApp app = new ExcelApp();
        app.start(args);
    }
}
