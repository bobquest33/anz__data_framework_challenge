
package com.priyab.anzchallenge;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;
import java.util.List;


/**
 * Class ColumnValidator
 */
public class ColumnValidator extends FileValidator {

  //
  // Fields
  //
  private Dataset<Row> dataSet;
  private Dataset<Row> columnSchema;

  
  //
  // Constructors
  //
  public ColumnValidator (Dataset<Row> dataSet, Dataset<Row> columnSchema) { 
    this.dataSet = dataSet;
    this.columnSchema = columnSchema;

  };
  
  //
  // Methods
  //
  /**
   * @param Dataset
   */
  public Boolean runValidation() {

    List<String> dscolList = new ArrayList<String>();

    // Iterate through the array
    for (String t : dataSet.columns()) {
      // Add each element into the list
      dscolList.add(t);
    }
    Long colSchemaCount = columnSchema.count();
    System.out.println("colSchemaCount: "+colSchemaCount);
    System.out.println("dscolList.size: "+dscolList.size());
    isValid = columnSchema.count() == dscolList.size();
    System.out.println("Column count matches");
    if(isValid == true) {
      colSchemaCount = columnSchema.filter(columnSchema.col("name").isin(dscolList.stream().toArray(String[]::new))).count();
      System.out.println("colSchemaCount col match: "+colSchemaCount);
      isValid = colSchemaCount == dscolList.size();
    }

    if(isValid == true) {
      failCode = 0;
    } else {
      failCode = 4;
    }
    System.out.println("Column test passed: "+isValid);
    return isValid;
  }


  //
  // Accessor methods
  //

  //
  // Other methods
  //

}
