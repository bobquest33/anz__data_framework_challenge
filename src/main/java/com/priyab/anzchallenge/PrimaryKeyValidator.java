
package com.priyab.anzchallenge;
import java.util.stream.Collectors;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Encoders;

/**
 * Class PrimaryKeyValidator
 */
public class PrimaryKeyValidator extends FileValidator {

  //
  // Fields
  //
  private Dataset<Row> dataSet;
  private List<String> primaryKeysList;
  private SparkSession spark;
  
  //
  // Constructors
  //
  public PrimaryKeyValidator (Dataset<Row> dataSet, List<String> primaryKeysList, SparkSession spark) {
    this.dataSet = dataSet;
    this.primaryKeysList = primaryKeysList;
    this.spark = spark;
  };
  
  //
  // Methods
  //
  /**
   * @param Dataset
   */
  public Boolean runValidation() {

    List<String> pkSqlList = primaryKeysList.stream().map(u -> "`"+u+"`").collect(Collectors.toList());
    String pkStr = String.join(",", pkSqlList);

    dataSet.createOrReplaceTempView("data_set");
    String queyString = "select "+pkStr+", count(*) as cnt from data_set group by "+ pkStr +" having cnt > 1;";
    Dataset<Row> dupRecords = spark.sql(queyString);
    isValid = dupRecords.count() == 0;
    if(isValid == true) {
      failCode = 0;
    } else {
      failCode = 3;
    }
    System.out.println("Primary Key test passed: "+isValid);
    return isValid;
  }


  //
  // Accessor methods
  //

  //
  // Other methods
  //

}
