package com.priyab.anzchallenge;
import static org.apache.spark.sql.functions.explode;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

/**
 * Class SchemaFile
 */
public class SchemaFile extends DataFile{

  //
  // Fields
  //
  private List<String> primaryKeysList;
  private Dataset<Row> columnSchemas;

  public Dataset<Row> getColumnSchemas() {
    return this.columnSchemas;
  }

  public void setColumnSchemas(Dataset<Row> columnSchemas) {
    this.columnSchemas = columnSchemas;
  }
  //

  public List<String> getPrimaryKeysList() {
    return this.primaryKeysList;
  }

  public void setPrimaryKeysList(List<String> primaryKeysList) {
    this.primaryKeysList = primaryKeysList;
  }
  // Constructors
  //
  public SchemaFile (String fileName) { 
    this.filePath = fileName;
  };
  
  //
  // Methods
  //

  public void extractSchema() {
    Dataset<Row> df = this.dataset;
    this.columnSchemas = df.withColumn("columns", explode(df.col("columns"))).select("columns.*").distinct();
    this.columnSchemas.printSchema();

  }

  public void extractPrimaryKeys() {
    // List primaryKeysList = this.dataset.select("primary_keys")dataFrame.rdd.map(r => r(0)).collect()
    Dataset<Row> df = this.dataset;
    primaryKeysList = new ArrayList<String>();
    List<Row> pkDf = df.withColumn("primaryKeys", explode(df.col("primary_keys"))).select("primaryKeys").collectAsList();
    for (Row r : pkDf) {
      System.out.println("Primary Keys:"+r.toString());
      primaryKeysList.add(r.getString(0));
    }
    
  }

  public void loadFile(SparkSession spark) {
    this.dataset = spark.read()
        .option("multiline","true")
        .format("json")
        .load(this.filePath);

  }

}
