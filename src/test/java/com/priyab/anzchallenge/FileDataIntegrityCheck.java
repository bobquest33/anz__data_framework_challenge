package com.priyab.anzchallenge;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.And;
import com.priyab.anzchallenge.*;
import static org.junit.Assert.assertTrue;

public class FileDataIntegrityCheck {
    private IngestDriver testIngestDriver;

    public FileDataIntegrityCheck() {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.sql.legacy.timeParserPolicy", "LEGACY");
        testIngestDriver = new IngestDriver();
 
    }


    @Given("^I have a DATA named \"([^\"]*)\"$")
    public void i_have_a_data_named_something(String strArg1) {
        System.out.println("Data file path is: " + strArg1);
        testIngestDriver.getInputFile().setFilePath(strArg1);
        return;
    }

    @And("^I have a Data TAG file named \"([^\"]*)\"$")
    public void i_have_a_tag_file_named_something(String strArg1) {
        System.out.println("Tag file path is: " + strArg1);
        testIngestDriver.setTagFile(new TagFile(strArg1));
        return;
    }

    @And("^I have a Data SCHEMA file named \"([^\"]*)\"$")
    public void i_have_a_schema_file_named_something(String strArg1) {
        System.out.println("Schema file path is: " + strArg1);
        testIngestDriver.setSchemaFile(new SchemaFile(strArg1));
        return;
    }

    @When("^I execute the Data application with output \"([^\"]*)\"$")
    public void i_execute_the_application_with_output_something(String strArg1) {
        System.out.println("Output file path is: " + strArg1);
        testIngestDriver.getOutputFile().setFilePath(strArg1);
        testIngestDriver.runDriver();
        return;
    }

    @Then("^the program should exist with RETURN CODE of 0$")
    public void the_program_should_exist_with_return_code_of_0() {
        assertTrue(testIngestDriver.getRunCode()==0);
        return;
    }

    @And("^\"([^\"]*)\" should match \"([^\"]*)\"$")
    public void something_should_match_something(String strArg1, String strArg2) {
        CsvDataFile opFile = new CsvDataFile();
        opFile.setFilePath(strArg1);
        opFile.loadFile(testIngestDriver.getSpark());
        CsvDataFile expFile = new CsvDataFile();
        expFile.setFilePath(strArg2);
        expFile.loadFile(testIngestDriver.getSpark());
        // Checking equality of two data frames using below link
        // https://stackoverflow.com/questions/31197353/dataframe-equality-in-apache-spark
        assertTrue(expFile.getDataset().schema().equals(opFile.getDataset().schema()));
        assertTrue(expFile.getDataset().count() ==  opFile.getDataset().count());
        assertTrue(expFile.getDataset().collectAsList().equals(opFile.getDataset().collectAsList()));
        return;
    }

}