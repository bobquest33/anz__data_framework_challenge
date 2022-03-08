
package com.priyab.anzchallenge;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;
import java.util.List;  
import java.util.Objects;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Class FieldValidator
 */
public class FieldValidator extends FileValidator {

  //
  // Fields
  //
  private Dataset<Row> dataSet;
  private Dataset<Row> columnSchema;
  Column dirtyFlag;
  private Dataset<Row> outDataSet;

  public Dataset<Row> getDataSet() {
    return this.dataSet;
  }

  public void setDataSet(Dataset<Row> dataSet) {
    this.dataSet = dataSet;
  }

  public Dataset<Row> getColumnSchema() {
    return this.columnSchema;
  }

  public void setColumnSchema(Dataset<Row> columnSchema) {
    this.columnSchema = columnSchema;
  }

  public Column getDirtyFlag() {
    return this.dirtyFlag;
  }

  public void setDirtyFlag(Column dirtyFlag) {
    this.dirtyFlag = dirtyFlag;
  }

  public Dataset<Row> getOutDataSet() {
    return this.outDataSet;
  }

  public void setOutDataSet(Dataset<Row> outDataSet) {
    this.outDataSet = outDataSet;
  }

  
  //
  // Constructors
  //
  public FieldValidator (Dataset<Row> dataSet, Dataset<Row> columnSchema) { 
    this.dataSet = dataSet;
    this.columnSchema = columnSchema;
    this.outDataSet = this.dataSet;

  };
 
  //
  // Methods
  //
  private void checkCondition(Row r) {
    String colName = (String) r.getAs("name");
    String colType = (String) r.getAs("type");
    Boolean colMandatory = (Boolean) r.getAs("mandatory");
    String colFormat = (String) r.getAs("format");

    StructField[] fields = dataSet.schema().fields();

    String dataType = null;

    for (StructField field : fields) {
      if (field.name().equals(colName)) {
        dataType = field.dataType().typeName();
        break;
      }
    }

    System.out.println(colName+"|"+colType+"|"+colMandatory+"|"+colFormat+"|"+dataType);

    if(colMandatory == true){
      if(Objects.isNull(this.dirtyFlag)) {
        this.dirtyFlag = when(col(colName).isNull(),lit(1));
      }
      else {
        this.dirtyFlag = this.dirtyFlag.when(col(colName).isNull(),lit(1));

      }
    }

    switch (colType){
    case "STRING":
      if(Objects.isNull(this.dirtyFlag)) {
        this.dirtyFlag = when(col(colName).isNotNull().cast(DataTypes.StringType).isNull(),lit(1));
      }
      else {
        this.dirtyFlag = this.dirtyFlag.when(col(colName).isNotNull().cast(DataTypes.StringType).isNull(),lit(1));

      }
    break;  
    case "INTEGER":
        if(Objects.isNull(this.dirtyFlag)) {
          this.dirtyFlag = when(col(colName).cast(DataTypes.IntegerType).isNull().and(col(colName).isNotNull()),lit(1));
        }
        else {
          this.dirtyFlag = this.dirtyFlag.when(col(colName).cast(DataTypes.IntegerType).isNull().and(col(colName).isNotNull()),lit(1));
  
        }
      break;  
      case "FLOAT":
      if(Objects.isNull(this.dirtyFlag)) {
          this.dirtyFlag = when(col(colName).cast(DataTypes.FloatType).isNull().and(col(colName).isNotNull()),lit(1));
        }
        else {
          this.dirtyFlag = this.dirtyFlag.when(col(colName).cast(DataTypes.FloatType).isNull().and(col(colName).isNotNull()),lit(1));
  
        }
      break;
      case "DATE":
      if(Objects.isNull(this.dirtyFlag)) {
          this.dirtyFlag = when(to_date(col(colName),colFormat).isNull().and(col(colName).isNotNull()),lit(1));
        }
        else {
          this.dirtyFlag = this.dirtyFlag.when(to_date(col(colName),colFormat).isNull().and(col(colName).isNotNull()),lit(1));
  
        }
      break;
      }

  }

  private void checkFields() {
    for(Row r: columnSchema.collectAsList()){
      checkCondition(r);

    }
    if(Objects.nonNull(this.dirtyFlag)) {
      this.dirtyFlag = this.dirtyFlag.otherwise(lit(0));
      this.outDataSet = this.dataSet.withColumn("dirty_flag", this.dirtyFlag);

    }
  }


  public Boolean runValidation() {

    checkFields();

    isValid = true;
    if(isValid == true) {
      failCode = 0;
    } else {
      failCode = 4;
    }
    System.out.println("Field test completed: "+isValid);
    return isValid;
  }


  //
  // Accessor methods
  //

  //
  // Other methods
  //

}
