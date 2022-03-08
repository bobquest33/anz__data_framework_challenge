
package com.priyab.anzchallenge;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Class FileNameValidator
 */
public class FileNameValidator extends FileValidator {

  //
  // Fields
  //
  private Dataset<Row> dataSet;
  private String dataFileName;

  
  //
  // Constructors
  //
  public FileNameValidator (Dataset<Row> tagData, String dataFileName) { 
    this.dataSet = tagData;
    this.dataFileName = dataFileName;
  };
  
  //
  // Methods
  //
  public Boolean runValidation() {
    isValid = dataSet.filter(dataSet.col("dataFileName").equalTo(dataFileName)).count() > 0;
    if(isValid == true) {
      failCode = 0;
    } else {
      failCode = 2;
    }
    System.out.println("File name test passed: "+isValid);
    return isValid;

  }

  //
  // Accessor methods
  //

  //
  // Other methods
  //

}
