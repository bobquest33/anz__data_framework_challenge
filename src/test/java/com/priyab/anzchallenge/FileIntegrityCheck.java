package com.priyab.anzchallenge;

import io.cucumber.java.en.Given;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.And;
import com.priyab.anzchallenge.*;
import static org.junit.Assert.assertTrue;


public class FileIntegrityCheck {
    private IngestDriver testIngestDriver;

    public FileIntegrityCheck() {
        System.setProperty("spark.master", "local");
        System.setProperty("spark.sql.legacy.timeParserPolicy", "LEGACY");
        testIngestDriver = new IngestDriver();

    }


    @Given("^I have a DATA file named \"([^\"]*)\"$")
    public void i_have_a_data_file_named_something(String strArg1) {
        System.out.println("Data file path is: " + strArg1);
        testIngestDriver.getInputFile().setFilePath(strArg1);
        return;
    }

    @And("^I have a TAG file named \"([^\"]*)\"$")
    public void i_have_a_tag_file_named_something(String strArg1) {
        System.out.println("Tag file path is: " + strArg1);
        testIngestDriver.setTagFile(new TagFile(strArg1));
        return;
    }

    @And("^I have a SCHEMA file named \"([^\"]*)\"$")
    public void i_have_a_schema_file_named_something(String strArg1) {
        System.out.println("Schema file path is: " + strArg1);
        testIngestDriver.setSchemaFile(new SchemaFile(strArg1));
        return;
    }

    @When("^I execute the application with output \"([^\"]*)\"$")
    public void i_execute_the_application_with_output_something(String strArg1) {
        System.out.println("Output file path is: " + strArg1);
        testIngestDriver.getOutputFile().setFilePath(strArg1);
        testIngestDriver.runDriver();
        return;
    }

    @Then("^the program should exit with RETURN CODE of \"([^\"]*)\"$")
    public void the_program_should_exit_with_return_code_of_something(String strArg1) {
        assertTrue(testIngestDriver.getRunCode()==Long.parseLong(strArg1));
        return;
    }

}