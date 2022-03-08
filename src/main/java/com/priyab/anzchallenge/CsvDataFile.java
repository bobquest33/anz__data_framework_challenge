
package com.priyab.anzchallenge;
import java.util.Objects;
import org.apache.spark.sql.SparkSession;

/**
 * Class CsvDataFile
 */
public class CsvDataFile extends DataFile {

  //
  // Fields
  //

  
  //
  // Constructors
  //
  public CsvDataFile () { };
  
  //
  // Methods
  //
  public void loadFile(SparkSession spark) {
    if (!Objects.isNull(this.fileSchema)) {
      this.dataset = spark.read().format("csv")
          .option("header", true)
          .option("multiline", true)
          .schema(this.fileSchema)
          .load(this.filePath);
    } else {
      this.dataset = spark.read().format("csv")
          .option("header", true)
          .option("multiline", true)
          .load(this.filePath);

    }

  }

  public void writeFile(SparkSession spark, String writeFileName)
  {
    this.dataset.coalesce(1).write().format("csv")
    .option("header", true)
    .option("path", writeFileName)
    .option("emptyValue", null)
    .option("nullValue", null)
    .mode("overwrite")
    .save();

  }

  //
  // Accessor methods
  //

  //
  // Other methods
  //

}
