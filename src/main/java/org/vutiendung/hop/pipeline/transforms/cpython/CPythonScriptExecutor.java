/*! ******************************************************************************
 *
 * CPython for the Hop orchestration platform
 *
 * http://www.project-hop.org
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.vutiendung.hop.pipeline.transforms.cpython;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopRowException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.Pipeline;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransform;
import org.apache.hop.pipeline.transform.ITransform;
import org.apache.hop.pipeline.transform.TransformMeta;
import org.apache.hop.pipeline.transform.stream.IStream;
import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.apache.hop.core.util.StreamLogger;

/**
 * Step that executes a python script using CPython. The step can accept 0 or more incoming row
 * sets. Row sets are sent to python as named pandas data frames. Data can be sent to python in
 * batches, as samples, row-by-row or as all available rows.
 * </p>
 * Output can be one or more variables that are set in python after the user's script executes. In
 * the case of a single variable this can be a data frame, in which case the columns of the frame
 * become output fields from this step. In the case of multiple variables they are retrieved in
 * string form or as png image data - the step automatically detects if a variable is an image and
 * retrieves it as png. In this mode there is one row output from the step, where each outgoing
 * field holds the string/serializable value of a single variable.
 * </p>
 * The step requires python 2.7 or 3.4. It also requires the pandas, numpy, matplotlib and sklearn.
 * The python executable must be available in the user's path.
 *
 */
public class CPythonScriptExecutor extends BaseTransform<CPythonScriptExecutorMeta, CPythonScriptExecutorData> {

  private static Class<?> PKG = CPythonScriptExecutorMeta.class;

  protected CPythonScriptExecutorData data;
  protected CPythonScriptExecutorMeta meta;

  protected boolean m_noInputRowSets = false;

  private Scanner outputFileReader;

  private String outputFilePath = "";

  private boolean firstRow = true;
  private String tempDir = "";
  private String lineSeparator = "";
  String delimiter = ",";
  String defaultDateFormat = "yyyy/MM/dd";
  String defaultTimestampFormat = "yyyy/MM/dd HH:mm:ss.SSSSSS";
  DateFormat dateFormater = new SimpleDateFormat(defaultDateFormat);
  String defautlPythonDateFormat = "%Y/%m/%d %H:%M:%S.%f";
  DateFormat timestampFormater = new SimpleDateFormat(defaultTimestampFormat);

  List<IRowSet> rowSets;
  List<String> inputFiles;
  List<String> inputFileSchema = new ArrayList<>();
  List<FileOutputStream> outputFileWriters;
  int numberOfInputStream = 0;
  int numberOfRowWrittenToOutput = 0;
  List<String> outputDateFileHeaders = new ArrayList<>();

  public CPythonScriptExecutor( TransformMeta transformMeta, CPythonScriptExecutorMeta meta,
      CPythonScriptExecutorData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) throws HopException {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    this.meta = meta;
    this.data = data;

    tempDir = System.getProperty("java.io.tmpdir");
    lineSeparator = System.getProperty("line.separator");

    outputFilePath = correctFilePath(tempDir + java.util.UUID.randomUUID() + "_output.csv");

    logDebug("Temp dir: " + tempDir);
    
    inputFiles = new ArrayList<String>();
    outputFileWriters = new ArrayList<>();

    for(int i = 0; i < meta.m_frameNames.size(); i ++) {
      String frameName = meta.m_frameNames.get(i);
      String filename = correctFilePath(tempDir  + java.util.UUID.randomUUID() + "_" + frameName +"_input.csv");
      inputFiles.add(filename);

      //Generate FileOutputStream to write data to output file
      try {
        FileOutputStream fileWriter = new FileOutputStream(filename);
        outputFileWriters.add(fileWriter);
      }
      catch (Exception ex) {
        throw new HopException( "There is an error when creating file writer object: " + ex.getMessage() ); //$NON-NLS-1$
      }
    }//end foreach frame name

    //Install library lib
    installPythonLibrary();
  }

  @Override public boolean processRow() throws HopException {
    
    Object[] currentRow = getRow();
    if(firstRow) {
      if(currentRow == null) {
        logBasic("There is not incoming row to this transformation!");
        setOutputDone();
        return false;
      }

      List<IStream> infoIStreams = meta.getStepIOMeta().getInfoStreams();
      for(int i=0; i< infoIStreams.size(); i ++) {
        String transformationName = infoIStreams.get(i).getSubject().toString();
        logDebug("Transformation name: " + transformationName);
        IRowMeta currentIRowMeta = getPipelineMeta().getTransformFields( variables, transformationName );
        String header = constructRowHeaderCsv(currentIRowMeta);
        FileOutputStream currentFileWriter = outputFileWriters.get(i);
        
        try {
          currentFileWriter.write(header.getBytes());
          currentFileWriter.flush();
        } catch (Exception e) {
          throw new HopException(e.getMessage());
        }

        //Generate pandas header
        String pandasHeader = generatePandasHeader(currentIRowMeta);
        inputFileSchema.add(pandasHeader);
      }

      firstRow = false;
    }

    //there is no more row
    //stop
    if(currentRow == null && !firstRow) {
      //close file first
      for(int i = 0; i < inputFiles.size(); i ++) {
        try {
          outputFileWriters.get(i).close();
        } catch (Exception e) {
          throw new HopException(e.getMessage());
        }
      }

      //execute stript
      try {
        rebuildScript();
      } catch (IOException e) {
        throw new HopException(e.getMessage());
      }

      //read output and put to next stream
      processFile(outputFilePath);

      //clean up temp file
      //cleanupTempFile();

      //
      setOutputDone();
      return false;
    }

    IRowSet currentRowSet = getInputRowSets().get(getCurrentInputRowSetNr());

    //write current row to all file of dataframe
    String currentInputStepName = currentRowSet.getOriginTransformName();
    for(int i = 0; i< meta.m_frameNames.size(); i ++) {

      FileOutputStream currentWriter = outputFileWriters.get(i);
      String inputStepname = meta.getStepIOMeta().getInfoStreams().get(i).getSubject().toString();

      if(currentInputStepName.equals(inputStepname)) {

        String csvRow = constructRowToCSV(currentRow, currentRowSet.getRowMeta());
        try {
            currentWriter.write(csvRow.getBytes());
          } catch (Exception ex) {
            throw new HopException(ex.getMessage());
          }
      }
    }
    return true;
    
  }

  private void rebuildScript() throws HopException, IOException {
    String prefScript =  "import pandas as pd" + lineSeparator + lineSeparator;
    //generate code to read input
    for(int i = 0; i < inputFiles.size(); i ++) {
      String frameName = meta.getFrameNames().get(i);
      String inputFileName = inputFiles.get(i);
      String dtypeString = inputFileSchema.get(i);

      prefScript = prefScript
                    + frameName + " = pd.read_csv(\"" + inputFileName + "\"" + dtypeString + ")"
                    + lineSeparator;
    }

    String outputDataFrame = meta.varListToString();

    String sufScript = "if '" + outputDataFrame +"' in locals():" + lineSeparator
    + "\t" + outputDataFrame + ".to_csv(\"" + outputFilePath + "\", index=False, date_format=\"" + defautlPythonDateFormat + "\")" + lineSeparator
    + "else:" + lineSeparator
    + "\tprint('Variable is not exist')" + lineSeparator ;

    String userScript = getUserScript();

    String finalScript = prefScript + userScript + lineSeparator + sufScript;
    String scriptPath = createScriptFile(finalScript);

    executeScriptFile(scriptPath);
  }

  private String constructRowToCSV(Object[] inputRow, IRowMeta currentRowMeta) throws HopValueException {
    String result = "";
    int numberOfInputField = currentRowMeta.size();

    for(int fieldIndex =0; fieldIndex < numberOfInputField; fieldIndex++) {
      IValueMeta curentValueMeta= currentRowMeta.getValueMeta(fieldIndex);
      String filedValue = objectToString(inputRow[fieldIndex], curentValueMeta);

      if ( fieldIndex <= numberOfInputField -2) {
        result = result + filedValue + delimiter;
      }
      else {
        result = result + filedValue + lineSeparator;
      }
    }//End foreach field

    return result;
  }

  private String constructRowHeaderCsv(IRowMeta currentRowMeta) {
    String result = "";
    int numberOfInputField = currentRowMeta.size();

    for (int fieldIndex = 0; fieldIndex < numberOfInputField; fieldIndex++) {
      IValueMeta fieldMeta = currentRowMeta.getValueMetaList().get(fieldIndex);
      String columnName = fieldMeta.getName();

      if (fieldIndex <= numberOfInputField - 2) {
        result = result + columnName + delimiter;
      } else {
        result = result + columnName + lineSeparator;
      }
    } // End foreach field

    return result;
  }

  private String createScriptFile(String script) throws HopException {
    String scriptPath = correctFilePath(tempDir + java.util.UUID.randomUUID() + "_script.py");
    writeStringToFile(script, scriptPath);
    return scriptPath;
  }

  private void executeScriptFile(String scriptPath) throws HopException {
    try {
      executeSystemCommand(new String[]{getExecutorPath(), scriptPath});

    } catch (Exception e) {
      throw new HopException(e.getMessage());
    } finally {
      File scriptFile = new File(scriptPath);
      if (scriptFile.exists()) {
        //scriptFile.delete();
      }
    }
  }

  private String getExecutorPath() {
    if(Const.getSystemOs().startsWith("Windows")) {
      return "python";
    }
    else {
      return "python3";
    }
  }

  private void processFile(String filePath) {
    System.out.println("Start reading output file of python");

    try {
      outputFileReader = new Scanner(new File(outputFilePath));
      int rowNumb = 0;
      IRowMeta outputFields = meta.m_outputFields;
      int numberOfField = outputFields.getValueMetaList().size();
      String[] outputFieldHeaders = null;

      while (outputFileReader.hasNextLine()) {
        String line = outputFileReader.nextLine();
        System.out.println(line);

        if (rowNumb == 0) {
          //parse header
          outputFieldHeaders = line.split(delimiter);
        }
        else {
          System.out.println("Output file header=" + Arrays.toString(outputFieldHeaders));
        
          String[] r = line.split(delimiter);
          Object[] outputRow = new Object[numberOfField];

          for(int i =0; i< numberOfField; i ++) {
            IValueMeta  field = outputFields.getValueMetaList().get(i);
            outputRow[i] = getValueOfField(field, outputFieldHeaders, r);
          }

          //System.out.println("Output row: " + Arrays.toString(outputRow));

          putRow(outputFields, outputRow);
        }

        rowNumb += 1;
      }
      outputFileReader.close();

    } catch (Exception e) {
      System.out.println("An error occurred.");
      e.printStackTrace();
    }
  }

  private String correctFilePath(String input) {
    if(Const.getSystemOs().startsWith("Windows")) {
      return input.replace("\\", "\\\\");
    }
    else {
      return input;
    }
  }

  private Object getValueOfField(IValueMeta fieldMeta, String[] fileHeader, String[] row ) throws HopException {
    try {
      int numberOfFieldFromFile = fileHeader.length;

      for(int i =0; i< numberOfFieldFromFile; i++) {
        if (fieldMeta.getName().equals(fileHeader[i])){
          ValueMetaString v = new ValueMetaString(row[i]);
          return fieldMeta.convertDataFromString(row[i], v, "", "", IValueMeta.TRIM_TYPE_NONE);
        }
      }

      logDebug("Cannot find field " + fieldMeta.getName() + " from array [" + java.util.Arrays.toString(fileHeader) + "], returning blank string");
      return null;

    } catch (Exception ex) {
      throw new HopException(ex.getMessage());
    }
  }

  private void cleanupTempFile() throws HopException {
    //delete input file
    for(int i = 0; i < inputFiles.size(); i ++) {
        File file = new File(inputFiles.get(i));
        if(file.exists()) {
          file.delete();
        }
    }

    //delete output file
    File outputFile = new File(outputFilePath);
    if (outputFile.exists()) {
      //outputFile.delete();
    }
  }

  private String getUserScript() throws HopException, IOException {
    if(meta.getLoadScriptAtRuntime()) {
      return readFileToString(meta.m_loadScriptFile);
    }
    else {
      return meta.getScript();
    }
  }

  private String readFileToString(String filePath) throws HopException, IOException {
    byte[] bytes = Files.readAllBytes(Paths.get(filePath));
    return new String (bytes);
  }

  private void writeStringToFile(String inputString, String filename) throws HopException {
    try {
      FileOutputStream  writer = new FileOutputStream(new File(filename));
      writer.write(inputString.getBytes());
      writer.close();
    }
    catch (Exception ex) {
      throw new HopException(ex.getMessage());
    }
  }

  private void executeSystemCommand(String[] param) throws HopException {
    ProcessBuilder processBuilder = new ProcessBuilder(param);

    try {

      Process process = processBuilder.start();

      // blocked :(
      BufferedReader readerOutput = new BufferedReader(new InputStreamReader(process.getInputStream()));
      BufferedReader readerError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

      String line;
      while ((line = readerOutput.readLine()) != null) {
        logDebug("Command output: " + line);
      }

      line = null;
      while ((line = readerError.readLine()) != null) {
        logError("Error when running commandline: " + line);
      }

      int exitCode = process.waitFor();
      logDebug("Return code of command: " + exitCode);
      
      if(exitCode > 0) {
        throw new HopException("There is an error when excuting script");
      }

    } catch (Exception e) {
        throw new HopException(e.getMessage());
    }
  }

  private void installPythonLibrary() throws HopException {
    logBasic("Installing python library");

    String libraryPath = Paths.get(tempDir, java.util.UUID.randomUUID() + "_library.txt").toString();

    if(meta.getLibrary() != null) {
      writeStringToFile(meta.getLibrary(), libraryPath);

      executeSystemCommand(new String[] {"pip", "install", "-r" , libraryPath});
    }
    else {
      logBasic("There is no config library, we will ignore this step.");
    }
    
  }

  private String objectToString(Object input, IValueMeta fieldMeta) throws HopValueException {
    if(input == null) {
      return "";
    }

    if(fieldMeta.isDate()) {
      return dateFormater.format(fieldMeta.getDate(input));
    }
    else if(fieldMeta.getType() == 9) {//timestamp type
      return timestampFormater.format(fieldMeta.getDate(input));
    }

    return fieldMeta.getString(input);
  }

  private String generatePandasHeader(IRowMeta rowMeta) {
    List<String> fieldHeader = new ArrayList<>();
    List<String> datetimeFieldToParse = new ArrayList<>();

    int countOfDatetimeField = 0;
    int countOfNonDatetimeField = 0;

    int numberOfInputField = rowMeta.size();

    if(numberOfInputField == 0 ) {
      return "";
    }

    for (int fieldIndex = 0; fieldIndex < numberOfInputField; fieldIndex++) {
      IValueMeta fieldMeta = rowMeta.getValueMetaList().get(fieldIndex);
      String columnName = fieldMeta.getName();
      String fieldType = fieldMeta.getTypeDesc();

      logDebug("Field name: " + columnName + ", filedType: " + fieldType);

      if(fieldType.equals("Date") || fieldType.equals("Timestamp")) {
        countOfDatetimeField++;
        datetimeFieldToParse.add("'" + columnName + "'");
        
      }
      else {
        countOfNonDatetimeField++;

        fieldHeader.add("'" + columnName + "': '" + hopeTypeToPandasType(fieldType) + "'");
      }
    } // End foreach field

    if(countOfNonDatetimeField == 0 && countOfDatetimeField ==0 ) {
      return "";
    }

    if(countOfNonDatetimeField == 0 && countOfDatetimeField > 0 ) {
      return ", parse_dates=[" + String.join(",", datetimeFieldToParse) + "]";
    }

    if(countOfNonDatetimeField > 0 && countOfDatetimeField == 0 ) {
      return ", dtype={" + String.join(",", fieldHeader) + "}";
    }

    if(countOfNonDatetimeField > 0 && countOfDatetimeField >= 0 ) {
      return ", dtype={" + String.join(",", fieldHeader) + "}"
          + ", parse_dates=[" + String.join(",", datetimeFieldToParse) + "]";
    }

    return "";
  }

  private String hopeTypeToPandasType(String hopType) {
    switch (hopType) {
      case "Boolean": return "bool";
      case "Date": return "datetime64";
      case "Integer": return "int64";
      case "BigNumber": return "int64";
      case "Timestamp": return "datetime64";
      case "String": return "str";
      case "Number": return "float64";
      default:
        return "object";
    }
  }
}