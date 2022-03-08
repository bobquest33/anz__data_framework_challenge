
package com.priyab.anzchallenge;

/**
 * Class FileValidator
 */
public class FileValidator {

  //
  // Fields
  //

  protected Integer failCode;
  protected Boolean isValid;


  public Boolean isIsValid() {
    return this.isValid;
  }

  public Boolean getIsValid() {
    return this.isValid;
  }

  public void setIsValid(Boolean isValid) {
    this.isValid = isValid;
  }

  //
  // Constructors
  //
  public FileValidator() {
  };

  //
  // Methods
  //

  //
  // Accessor methods
  //

  /**
   * Set the value of failCode
   * 
   * @param newVar the new value of failCode
   */
  public void setFailCode(Integer newVar) {
    failCode = newVar;
  }

  /**
   * Get the value of failCode
   * 
   * @return the value of failCode
   */
  public Integer getFailCode() {
    return failCode;
  }


  //
  // Other methods
  //

  /**
   * @param Dataset
   */
  public Boolean runValidation() {
    return true;
  }

}
