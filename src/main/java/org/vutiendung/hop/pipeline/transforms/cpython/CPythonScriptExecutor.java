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
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 */
public class CPythonScriptExecutor extends BaseTransform<CPythonScriptExecutorMeta, CPythonScriptExecutorData> {

  private static Class<?> PKG = CPythonScriptExecutorMeta.class;

  protected CPythonScriptExecutorData data;
  protected CPythonScriptExecutorMeta meta;

  protected boolean m_noInputRowSets = false;

  private Scanner outputFileReader;

  private String outputFilePath = "";
  private int currentRowNumb = 1;

  private boolean firstRow = true;
  private String tempDir = "";
  private String lineSeparator = "";
  String delimiter = ",";
  String defaultDateFormat = "yyyy-MM-dd";
  String defaultTimestampFormat = "yyyy-MM-dd HH:mm:ss.sss";
  DateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd");
  DateFormat timestampFormater = new SimpleDateFormat(defaultTimestampFormat);

  List<IRowSet> rowSets;
  List<String> inputFiles;
  List<FileOutputStream> outputFileWriters;
  List<IStream> infoStreams;
  int numberOfInputStream = 0;

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

    //Install library lib
    installPythonLibrary();
  }

  @Override public boolean processRow() throws HopException {
    int numberOfInputHasData = 0;
    boolean shouldWriteHeader = false;

    if (firstRow) {
      firstRow = false;

      shouldWriteHeader = true;

      //Get list of input stream
      infoStreams = meta.getTransformIOMeta().getInfoStreams();
      numberOfInputStream = infoStreams.size();

      if(numberOfInputStream == 0) {
        logBasic("There is no input stream configured!");
        setOutputDone();
        return false;
      }

      rowSets = new ArrayList<>();
      outputFileWriters = new ArrayList<>();

      logDebug("Found " + numberOfInputStream + " input stream(s)");

      for(int i = 0; i< numberOfInputStream; i++) {
        String transformationName = infoStreams.get(i).getSubject().toString();
        String frameName = meta.getFrameNames().get(i);

        IRowSet rowSet = findInputRowSet(transformationName);

        if(rowSet == null) {
          throw new HopException( BaseMessages
                                  .getString( PKG, "CPythonScriptExecutor.Error.UnableToFindSpecifiedInputStep",
                                  transformationName ) );
        }
        else {
           rowSets.add(rowSet);
        }
       
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

        waitUntilPipelineIsStarted();

      }//end foreach stream input list
    } //end if(firstRow)

    // Start processing row
    logDebug("Writing input data to temp file with row number: " + currentRowNumb);
    for(int i=0; i < numberOfInputStream; i++ ){
      String transformationName = infoStreams.get(i).getSubject().toString();      

      logDebug("Getting data for input stream " + transformationName);
      

      IRowSet currentRowSet = rowSets.get(i);
      Object[] r = getRowFrom(currentRowSet);

      logDebug("Input stream [" + transformationName +"] status: isDone=" + currentRowSet.isDone() + ", isBlocking=" + currentRowSet.isBlocking());

      if (r == null) {
        logDebug("The data of output [" + transformationName + "] is null");
        continue;
      }

      //get writer to write to temp file
      FileOutputStream writer = outputFileWriters.get(i);

      //It means this input has data
      numberOfInputHasData  += 1;

      String outputRowString = "";
      String headerString = "";

      IRowMeta associatedRowMeta  = getPipelineMeta().getTransformFields(variables, transformationName);
      int numberOfInputField = associatedRowMeta.size();

      for(int fieldIndex =0; fieldIndex < numberOfInputField; fieldIndex++) {
        IValueMeta fieldMeta = associatedRowMeta.getValueMetaList().get(fieldIndex);
        String columnName = fieldMeta.getName();
        
        //Header
        if ( fieldIndex <= numberOfInputField -2) {
            headerString = headerString + columnName + delimiter;
          }
          else {
            headerString = headerString + columnName + lineSeparator;
          }

          //value

          String filedValue = objectToString(r[fieldIndex], fieldMeta);
          if ( fieldIndex <= numberOfInputField -2) {
            outputRowString = outputRowString + filedValue + delimiter;
          }
          else {
            outputRowString = outputRowString + filedValue + lineSeparator;
          }

      }//End foreach field

      // Write to temp file
      try {
        if (shouldWriteHeader) {
          writer.write(headerString.getBytes());
        }

        writer.write(outputRowString.getBytes());
      } catch (Exception ex) {
        throw new HopException(ex.getMessage());
      }
      
    }//end foreach input stream

    

    //return false if there is no data to processs
    //If return true, hop will trigger this function again
    logDebug("numberOfInputHasData: " + numberOfInputHasData);
    currentRowNumb += 1;

    //Finished all the output
    //Process the output
    if (numberOfInputHasData == 0 ){
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

      //Close all connection
      for(int i = 0; i < outputFileWriters.size(); i++) {
        try {
          outputFileWriters.get(i).close();
        }
        catch(Exception ex) {
          throw new HopException(ex.getMessage());
        }
      }

      //tell next step that this step was done
      setOutputDone();
    }

    return numberOfInputHasData  > 0;
  }

  private void rebuildScript() throws HopException, IOException {
    String prefScript =  "import pandas as pd" + lineSeparator + lineSeparator;
    //generate code to read input
    for(int i = 0; i < numberOfInputStream; i ++) {
      String frameName = meta.getFrameNames().get(i);
      String inputFileName = inputFiles.get(i);

      prefScript = prefScript
                    + frameName + " = pd.read_csv(\"" + inputFileName + "\")"
                    + lineSeparator;
    }

    String outputDataFrame = meta.varListToString();

    String sufScript = "if '" + outputDataFrame +"' in locals():" + lineSeparator
    + "\t" + outputDataFrame + ".to_csv(\"" + outputFilePath + "\", index=False)" + lineSeparator
    + "else:" + lineSeparator
    + "\tprint('Variable is not exist')" + lineSeparator ;

    String userScript = getUserScript();

    String finalScript = prefScript + userScript + lineSeparator + sufScript;
    String scriptPath = createScriptFile(finalScript);

    executeScriptFile(scriptPath);
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
    writeStringToFile(meta.getLibrary(), libraryPath);

    executeSystemCommand(new String[] {"pip", "install", "-r" , libraryPath});
  }

  private String objectToString(Object input, IValueMeta fieldMeta) throws HopValueException {
    if(fieldMeta.isDate()) {
      return dateFormater.format(fieldMeta.getDate(input));
    }
    else if(fieldMeta.getType() == 9) {//timestamp type
      return timestampFormater.format(fieldMeta.getDate(input));
    }

    return String.valueOf(input);
  }
}