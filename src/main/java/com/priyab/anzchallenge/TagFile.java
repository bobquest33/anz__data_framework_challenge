package com.priyab.anzchallenge;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Class TagFile
 */
public class TagFile extends DataFile{

  //
  // Fields
  //
  
  //
  // Constructors
  //
  public TagFile (String fileName) { 
    this.filePath = fileName; 
    this.fileSchema = DataTypes.createStructType(new StructField[] { 
      DataTypes.createStructField(
      "dataFileName",
      DataTypes.StringType,
      false),
      DataTypes.createStructField(
        "recordCount", 
        DataTypes.LongType, 
        false)});
  };
  
  //
  // Methods
  //
  public void loadFile(SparkSession spark) {
    this.dataset = spark.read().format("csv")
    .option("header", false)
    .option("multiline", true)
    .option("sep", "|")
    .option("dateFormat", "MM/dd/yyyy")
    .option("mode", "FAILFAST")
    .schema(this.fileSchema) 
    .load(this.filePath);

  }  

  //
  // Accessor methods
  //

  //
  // Other methods
  //

}
