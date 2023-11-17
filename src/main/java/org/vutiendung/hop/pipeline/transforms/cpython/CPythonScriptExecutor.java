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
import org.apache.hop.core.row.IValueMetaConverter;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.ValueMetaAndData;
import org.apache.hop.core.row.value.ValueMetaConverter;
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  String defaultDateFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";
  String defaultTimestampFormat = "yyyy-MM-dd HH:mm:ss.SSSSSS";
  SimpleDateFormat dateFormater = new SimpleDateFormat(defaultDateFormat);
  String defautlPythonDateFormat = "%Y-%m-%d %H:%M:%S.%f";
  String defautlPythonDatetimeFormat = "%Y-%m-%d %H:%M:%S.%f";
  SimpleDateFormat timestampFormater = new SimpleDateFormat(defaultTimestampFormat);

  List<IRowSet> rowSets;
  List<String> inputFiles;
  List<String> inputFileSchema = new ArrayList<>();
  List<FileOutputStream> outputFileWriters;
  int numberOfInputStream = 0;
  int numberOfRowWrittenToOutput = 0;
  List<String> outputDateFileHeaders = new ArrayList<>();
  List<String> pythonConvertDatetimeCommands = new ArrayList<>();

  public CPythonScriptExecutor( TransformMeta transformMeta, CPythonScriptExecutorMeta meta,
      CPythonScriptExecutorData data, int copyNr, PipelineMeta pipelineMeta, Pipeline pipeline ) throws HopException {
    super( transformMeta, meta, data, copyNr, pipelineMeta, pipeline );

    this.meta = meta;
    this.data = data;

    tempDir = System.getProperty("java.io.tmpdir");
    lineSeparator = System.getProperty("line.separator");

    outputFilePath = correctFilePath(Paths.get(tempDir, java.util.UUID.randomUUID() + "_output.csv").toString());
    
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
        logBasic("There is no incoming row to this transformation!");
        setOutputDone();
        return false;
      }

      if(meta.m_frameNames.size() == 0) {
        logBasic("You are not configured any input dataframe. This step will be ignored!");

        setOutputDone();
        return false;
      }

      logDebug("Output frame name=[" + meta.varListToString() + "]");
      logDebug("--------------------------------------");
      if(meta.varListToString() == null || meta.varListToString().trim().isEmpty()){
        logBasic("You are not configured the name of python pandas to get value to output. The script will be ignored!");
        setOutputDone();
        return false;
      }

      IRowMeta outputFields = meta.m_outputFields;

      if(outputFields == null || outputFields.size() == 0) {
        logBasic("You are not configured any output field. The ouput will be blank!");

        setOutputDone();
        return false;
      }

      List<IStream> infoIStreams = meta.getStepIOMeta().getInfoStreams();
      for(int i=0; i< infoIStreams.size(); i ++) {
        String transformationName = infoIStreams.get(i).getSubject().toString();
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
        pythonConvertDatetimeCommands.add(generatePandasConvertDatetime(currentIRowMeta, meta.m_frameNames.get(i)));
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
      cleanupTempFile();

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
    String prefScript = "import pandas as pd" + lineSeparator
                      + "from datetime import date, datetime" + lineSeparator
                      + "from pandas.api.types import is_datetime64_any_dtype" + lineSeparator
                      + lineSeparator;
    //generate code to read input
    for(int i = 0; i < inputFiles.size(); i ++) {
      String frameName = meta.getFrameNames().get(i);
      String inputFileName = inputFiles.get(i);
      String dtypeString = inputFileSchema.get(i);

      prefScript = prefScript
                    + frameName + " = pd.read_csv(\"" + inputFileName + "\"" + dtypeString + ")"
                    + lineSeparator
                    + pythonConvertDatetimeCommands.get(i)
                    + lineSeparator;
    }

    prefScript = prefScript + lineSeparator + "# Start user's script" + lineSeparator + lineSeparator;

    String outputDataFrame = meta.varListToString();

    String sufScript = lineSeparator + "# End of user's script" + lineSeparator + lineSeparator
//Function to convert any type to string
                                      + "def objectToString(inputObject):" + lineSeparator
                                      + "\tif isinstance(inputObject, str):" + lineSeparator
                                      + "\t\treturn inputObject" + lineSeparator
                                      + "\tif isinstance(inputObject, date):" + lineSeparator
                                      + "\t\treturn inputObject.strftime(\"%Y%m%d\")" + lineSeparator
                                      + "\tif isinstance(inputObject, datetime):" + lineSeparator
                                      + "\t\treturn inputObject.strftime(\"%Y%M%D_%H%M%S\")" + lineSeparator
                                      + "\tif isinstance(inputObject, int) or isinstance(inputObject, float) or isinstance(inputObject, bool) :" + lineSeparator
                                      + "\t\treturn str(inputObject)" + lineSeparator
                                      + "\telse:" + lineSeparator
                                      + "\t\treturn \"\"" + lineSeparator + lineSeparator
//Check if variable is in script
                                      + "if not '" + outputDataFrame +"' in locals():" + lineSeparator
                                      + "\tprint('Variable [" + outputDataFrame + "] is not exist. Please check the script or job config again!')" + lineSeparator
                                      + "\texit(1)" + lineSeparator + lineSeparator
//Reseting index,column
                                      + "if " + outputDataFrame + ".columns.nlevels > 1:" + lineSeparator
                                      + "\tprint(\"Reseting column\")" + lineSeparator
                                      + "\t" + outputDataFrame + ".columns = " + outputDataFrame + ".columns.map(lambda x: '_'.join([objectToString(i) for i in x]))" + lineSeparator + lineSeparator

                                      + "print(\"Reseting index\")" + lineSeparator
                                      + outputDataFrame + ".reset_index(inplace=True)" + lineSeparator + lineSeparator
//Conver timestamp to string
                                      + "for series_name in " + outputDataFrame + ":" + lineSeparator
                                      + "\tif is_datetime64_any_dtype(" + outputDataFrame + "[series_name].dtype):" + lineSeparator
                                      + "\t\t" + outputDataFrame + "[series_name] = " + outputDataFrame + "[series_name].dt.strftime(\"" + defautlPythonDatetimeFormat + "\")" + lineSeparator + lineSeparator
//Export dataframe to file
                                      + outputDataFrame + ".to_csv(\"" + outputFilePath + "\", index=False, date_format=\"" + defautlPythonDateFormat + "\")" + lineSeparator
//End script
                                      + lineSeparator;
                                     

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
    logDebug("Start reading output data of python script");

    try {
      outputFileReader = new Scanner(new File(outputFilePath));
      int rowNumb = 0;
      IRowMeta outputFields = meta.m_outputFields;

      int numberOfField = outputFields.getValueMetaList().size();
      String[] outputFieldHeaders = null;

      while (outputFileReader.hasNextLine()) {
        String line = outputFileReader.nextLine();

        if (rowNumb == 0) {
          //parse header
          outputFieldHeaders = line.split(delimiter);
        }
        else {
        
          String[] r = line.split(delimiter);
          Object[] outputRow = new Object[numberOfField];

          for(int i =0; i< numberOfField; i ++) {
            IValueMeta  field = outputFields.getValueMetaList().get(i);
            outputRow[i] = getValueOfField(field, outputFieldHeaders, r);
          }

          putRow(outputFields, outputRow);
        }

        rowNumb += 1;
      }
      outputFileReader.close();

    } catch (Exception e) {
      logError(e.getMessage());
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

        if(i >= row.length) {
          return null;
        }

        if (fieldMeta.getName().equals(fileHeader[i])){
          ValueMetaString fromFieldMeta = new ValueMetaString(row[i]);
          ValueMetaConverter converter = new ValueMetaConverter();
          String fieldValue = row[i];

          if(fieldMeta.getType() == 9) { // Timestamp
            converter.setDatePattern(timestampFormater);
          }

          if(fieldMeta.getType() == 3) { // Date
            if(stringMatch(row[i], "\\d{4}-\\d{2}-\\d{2}")) {
              fieldValue += " 00:00:00.000000";
              SimpleDateFormat formater = new SimpleDateFormat("yyyy-MM-dd");
              converter.setDatePattern(formater);
            }
            else {
              converter.setDatePattern(dateFormater);
            } 
          }

          return converter.convertFromSourceToTargetDataType(fromFieldMeta.getType(), fieldMeta.getType(), (Object)fieldValue);
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

      BufferedReader readerOutput = new BufferedReader(new InputStreamReader(process.getInputStream()));
      BufferedReader readerError = new BufferedReader(new InputStreamReader(process.getErrorStream()));

      String line;
      while ((line = readerOutput.readLine()) != null) {
        logBasic("Command output: " + line);
      }

      line = null;
      while ((line = readerError.readLine()) != null) {
        logError("Error when running commandline: " + line);
      }

      int exitCode = process.waitFor();
      logBasic("Return code of command: " + exitCode);
      
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

    if(fieldMeta.getType() == 3) { //Date
      return dateFormater.format(input);
    }

    return fieldMeta.getString(input);
  }

  private String generatePandasHeader(IRowMeta rowMeta) {
    List<String> fieldHeader = new ArrayList<>();
    int countOfNonDatetimeField = 0;

    int numberOfInputField = rowMeta.size();

    if(numberOfInputField == 0 ) {
      return "";
    }

    for (int fieldIndex = 0; fieldIndex < numberOfInputField; fieldIndex++) {
      IValueMeta fieldMeta = rowMeta.getValueMetaList().get(fieldIndex);
      String columnName = fieldMeta.getName();
      String fieldType = fieldMeta.getTypeDesc();

      if(!fieldType.equals("Date") && !fieldType.equals("Timestamp")) { 
        countOfNonDatetimeField ++;
        fieldHeader.add("'" + columnName + "': '" + hopeTypeToPandasType(fieldType) + "'");
      }
    } // End foreach field

    if(countOfNonDatetimeField > 0) {
      return ", dtype={" + String.join(",", fieldHeader) + "}";
    }
    return "";
  }

  private String generatePandasConvertDatetime(IRowMeta rowMeta, String dataFrameName) {
    String result = "";

    int numberOfInputField = rowMeta.size();

    if(numberOfInputField == 0 ) {
      return "";
    }

    for (int fieldIndex = 0; fieldIndex < numberOfInputField; fieldIndex++) {
      IValueMeta fieldMeta = rowMeta.getValueMetaList().get(fieldIndex);
      String columnName = fieldMeta.getName();
      
      if(fieldMeta.getType() == 3) {  //Date
         result = result + dataFrameName + "['"+ columnName +"'] = pd.to_datetime(" + dataFrameName +"['" + columnName +"'], format=\"" + defautlPythonDatetimeFormat + "\")" + lineSeparator;
      }

    } // End foreach field
    return result + lineSeparator;
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

  private boolean stringMatch(String input, String regex) {
    final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
    final Matcher matcher = pattern.matcher(input);
    
    while (matcher.find()) {
        return true;
    }
    return false;
  }
}