package com.priyab.anzchallenge;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.SparkSession;

/**
 * Class IngestDriver
 */
public class IngestDriver {

  //
  // Fields
  //

  private DataFile inputFile;
  private DataFile outputFile;
  private SchemaFile schemaFile;
  private TagFile tagFile;
  private Integer runCode;
  private SparkSession spark;

  public Integer getRunCode() {
    return this.runCode;
  }

  public void setRunCode(Integer runCode) {
    this.runCode = runCode;
  }

  public SchemaFile getSchemaFile() {
    return this.schemaFile;
  }

  public void setSchemaFile(SchemaFile schemaFile) {
    this.schemaFile = schemaFile;
  }

  public TagFile getTagFile() {
    return this.tagFile;
  }

  public void setTagFile(TagFile tagFile) {
    this.tagFile = tagFile;
  }

  public SparkSession getSpark() {
    return this.spark;
  }

  public void setSpark(SparkSession spark) {
    this.spark = spark;
  }
  
  //
  // Constructors
  //
  public IngestDriver () { 
    
    inputFile = new CsvDataFile();
    outputFile = new CsvDataFile();
    runCode = 0;

  };
  
  //
  // Methods
  //

    /**
   * The processing code.
   */
  private void startDriver() {
    // Creates a session on a local master
    spark = SparkSession.builder()
        .appName("CSV to Dataset")
        .getOrCreate();

  }

  //
  // Accessor methods
  //

  /**
   * Set the value of inputFile
   * @param newVar the new value of inputFile
   */
  public void setInputFile (DataFile newVar) {
    inputFile = newVar;
  }

  /**
   * Get the value of inputFile
   * @return the value of inputFile
   */
  public DataFile getInputFile () {
    return inputFile;
  }

  /**
   * Set the value of outputFile
   * @param newVar the new value of outputFile
   */
  public void setOutputFile (DataFile newVar) {
    outputFile = newVar;
  }

  /**
   * Get the value of outputFile
   * @return the value of outputFile
   */
  public DataFile getOutputFile () {
    return outputFile;
  }

  //
  // Other methods
  //

  /**
   */
  public void loadDataFile()
  {
    inputFile.loadFile(spark);
  }


  /**
   */
  public void loadTagFile()
  {
    tagFile.loadFile(spark);
  }


  /**
   */
  public void loadSchemaFile()
  {
    schemaFile.loadFile(spark);
  }


  public Boolean isValid(FileValidator validator){
    Boolean isValid = validator.runValidation();
    runCode = validator.getFailCode();
    return isValid;
  }

  /**
   */
  public Boolean checkRecordCount()
  {
    String fileName = FilenameUtils.getName(inputFile.getFilePath());
    RecordCountValidator validator = new RecordCountValidator(tagFile.getDataset(), inputFile.getDataset().count(), fileName);
    return isValid(validator);

  }


  /**
   */
  public Boolean checkFileName()
  {
    String fileName = FilenameUtils.getName(inputFile.getFilePath());
    FileNameValidator validator = new FileNameValidator(tagFile.getDataset(), fileName);
    return isValid(validator);

  }


  /**
   */
  public Boolean checkPrimaryKey()
  {
    schemaFile.extractPrimaryKeys();
    System.out.println("Primary Key List:");
    System.out.println(schemaFile.getPrimaryKeysList().toString());
    PrimaryKeyValidator validator = new PrimaryKeyValidator(inputFile.getDataset(), schemaFile.getPrimaryKeysList(), spark);
    return isValid(validator);

  }


  /**
   */
  public Boolean validateColumns()
  {
    schemaFile.extractSchema();
    ColumnValidator validator = new ColumnValidator(inputFile.getDataset(), schemaFile.getColumnSchemas());
    return isValid(validator);
  }


  /**
   */
  public void validateFields()
  {
    FieldValidator validator = new FieldValidator(inputFile.getDataset(), schemaFile.getColumnSchemas());
    isValid(validator);
    this.outputFile.setDataset(validator.getOutDataSet());
  }


  /**
   */
  public void writeOutput()
  {
    outputFile.writeFile(spark, outputFile.getFilePath());

  }

/**
   */
  public void printFiles(){
    tagFile.printData();
    schemaFile.printData();
    inputFile.printData();

  }
  

  /**
   */
  public Boolean runDriver()
  {
    startDriver();
    loadSchemaFile();
    loadTagFile();
    loadDataFile();
    printFiles();
    if(!checkRecordCount()) {
      return false;
   }
   if(!checkFileName()) {
    return false;
   }
   if(!checkPrimaryKey()){
     return false;
   }
   if(!validateColumns()){
    return false;
   }
   validateFields();
   writeOutput();
   return true;
  }


  /**
   */
  public void parseHelper(String[] args)
  {
      Options options = new Options();
      Option schema = new Option("schema", true, "Schema File Path");
      schema.setRequired(true);
      options.addOption(schema);
      Option data = new Option("data", true, "Input Data File Path");
      data.setRequired(true);
      options.addOption(data);
      Option tag = new Option("tag", true, "Tag File Path");
      tag.setRequired(true);
      options.addOption(tag);
      Option output = new Option("output", true, "Output Data File Path");
      output.setRequired(true);
      options.addOption(output);
      HelpFormatter formatter = new HelpFormatter();
      CommandLineParser parser = new DefaultParser();
      CommandLine cmd;
      try {
          cmd = parser.parse(options, args);
      } catch (ParseException e) {
          System.out.println(e.getMessage());
          formatter.printHelp("CSV Data Ingest Tool Help", options);
          System.exit(1);
          return;
      }

      System.out.println("Data file path is: " + cmd.getOptionValue("data"));
      inputFile.setFilePath(cmd.getOptionValue("data"));
      System.out.println("Data file path is: " + cmd.getOptionValue("output"));
      outputFile.setFilePath(cmd.getOptionValue("output"));
      System.out.println("Schema file path is: " + cmd.getOptionValue("schema"));
      this.setSchemaFile(new SchemaFile(cmd.getOptionValue("schema")));
      System.out.println("Tag file path is: " + cmd.getOptionValue("tag"));
      this.setTagFile(new TagFile(cmd.getOptionValue("tag")));


  }



}
