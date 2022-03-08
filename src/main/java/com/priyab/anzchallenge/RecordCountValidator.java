
package com.priyab.anzchallenge;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class RecordCountValidator
 */
public class RecordCountValidator extends FileValidator {

  //
  // Fields
  //
  private Dataset<Row> dataSet;
  private Long recordCount;
  private String dataFileName;
  
  //
  // Constructors
  //
  public RecordCountValidator (Dataset<Row> tagData, Long recordCount, String dataFileName) { 
    this.dataSet = tagData;
    this.recordCount = recordCount;
    this.dataFileName = dataFileName;
  };
  
  //
  // Methods
  //
  /**
   * @param Dataset
   */
  public Boolean runValidation() {
    // Dataset<Row> dataTest = dataSet.filter(dataSet.col("recordCount").equalTo(recordCount).and(dataSet.col("dataFileName").equalTo(dataFileName)));
    Dataset<Row> dataTest = dataSet.filter(dataSet.col("recordCount").equalTo(recordCount));
    isValid =  dataTest.count() > 0;
    if(isValid == true) {
      failCode = 0;
    } else {
      failCode = 1;
    }
    System.out.println("Record count test passed: "+isValid);
    return isValid;
  }

  //
  // Accessor methods
  //

  //
  // Other methods
  //

}
