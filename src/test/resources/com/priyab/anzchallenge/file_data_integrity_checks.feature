Feature: Field (Data) Integrity checks
These tests are typically performed to test the quality of the data

  Scenario: Valid fields
    Given I have a DATA named "scenarios/aus-capitals.csv"
      And I have a Data TAG file named "scenarios/aus-capitals.tag"
      And I have a Data SCHEMA file named "scenarios/aus-capitals.json"
     When I execute the Data application with output "output/act-sbe2-1.csv"
     Then the program should exist with RETURN CODE of 0
      And "output/act-sbe2-1.csv" should match "scenarios/exp-sbe2-1.csv"
  Scenario: Invalid fields
    Given I have a DATA named "scenarios/aus-capitals-invalid-3.csv"
      And I have a Data TAG file named "scenarios/aus-capitals-invalid-3.tag"
      And I have a Data SCHEMA file named "scenarios/aus-capitals.json" 
     When I execute the Data application with output "output/act-sbe2-2.csv"
     Then the program should exist with RETURN CODE of 0
      And "output/act-sbe2-2.csv" should match "scenarios/exp-sbe2-2.csv"
