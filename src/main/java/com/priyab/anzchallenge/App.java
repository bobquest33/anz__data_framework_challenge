package com.priyab.anzchallenge;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void exit(Integer runCode){
        System.out.println("Exiting with code: "+runCode);
        System.exit(runCode);

    }
    public static void main( String[] args )
    {
        IngestDriver ingestDriver = new IngestDriver();
        ingestDriver.parseHelper(args);
        ingestDriver.runDriver();
        exit(ingestDriver.getRunCode());
    }
}
