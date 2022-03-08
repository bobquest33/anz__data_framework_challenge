package com.priyab.anzchallenge;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.SparkSession;

/**
 * Class DataFile
 */
public class DataFile {

  //
  // Fields
  //

  /**
   * This attribute refers to the Dataset object object read from the file.
   */
  protected Dataset<Row> dataset;
  /**
   * Path of the Data file.
   */
  protected String filePath;
  /**
   * The obejct to the Schema for dataset
   */
  protected StructType fileSchema;
  
  //
  // Constructors
  //
  public DataFile () { };
  
  //
  // Methods
  //
  public void printData() {
    System.out.println("File Name:"+filePath);
    System.out.println("Excerpt of the dataframe content:");
    this.dataset.show();
    System.out.println("Dataframe's schema:");
    this.dataset.printSchema();
  }

  //
  // Accessor methods
  //

  /**
   * Set the value of Dataset
   * This attribute refers to the Dataset object object read from the file.
   * @param newVar the new value of Dataset
   */
  public void setDataset (Dataset<Row> newVar) {
    dataset = newVar;
  }

  /**
   * Get the value of Dataset
   * This attribute refers to the Dataset object object read from the file.
   * @return the value of Dataset
   */
  public Dataset<Row> getDataset () {
    return dataset;
  }

  /**
   * Set the value of filePath
   * Path of the file.
   * @param newVar the new value of filePath
   */
  public void setFilePath (String newVar) {
    filePath = newVar;
  }

  /**
   * Get the value of filePath
   * Path of the file.
   * @return the value of filePath
   */
  public String getFilePath () {
    return filePath;
  }

  /**
   * Set the value of fileSchema
   * The obejct to the Schema for the CSV File
   * @param newVar the new value of fileSchema
   */
  public void setFileSchema (StructType newVar) {
    fileSchema = newVar;
  }

  /**
   * Get the value of fileSchema
   * The obejct to the Schema for the CSV File
   * @return the value of fileSchema
   */
  public StructType getFileSchema () {
    return fileSchema;
  }

  //
  // Other methods
  //

  /**
   */
  public void loadFile(SparkSession spark)
  {
  }


  /**
   */
  public void writeFile(SparkSession spark, String writeFileName)
  {
  }


}
