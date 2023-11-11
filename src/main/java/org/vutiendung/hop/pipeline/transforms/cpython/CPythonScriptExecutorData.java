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

import org.apache.commons.vfs2.FileObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hop.core.IRowSet;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowDataUtil;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transform.BaseTransformData;
import org.apache.hop.pipeline.transform.ITransformData;

/**
 * Data class for the CPythonScriptExecutor step
 *
 * @author Mark Hall (mhall{[at]}phalanxdev{[dot]}com)
 */
public class CPythonScriptExecutorData extends BaseTransformData implements ITransformData {

  private static Class<?> PKG = CPythonScriptExecutorMeta.class;

  /**
   * number of rows to randomly generate when trying to determine single pandas data frame output
   */
  protected static final int NUM_RANDOM_ROWS = 100;

  /**
   * The reservoir sampling class does not have "store all rows" behavior when the sample size is
   * -1. Instead, it is disabled entirely when sample size < 0. So to simulate this behavior we use
   * a default size for the -1 case. If this is not sufficient, then the user will have to manually
   * set a size that is large enough. Note that Integer.MAX_VALUE is not used because the reservoir
   * class allocates an array list of size equal to the sample size.
   */
  protected static final int DEFAULT_RESERVOIR_SAMPLING_STORE_ALL_ROWS_SIZE = 100000;

  /**
   * Holds the full output row meta data (including any incoming fields that are copied to the
   * outgoing)
   */
  public IRowMeta m_outputRowMeta;

  /**
   * Holds output row meta for fields only generated from script execution
   */
  public IRowMeta m_scriptOnlyOutputRowMeta;

  /**
   * Holds the row meta for all incoming fields that are getting copied to the output
   */
  public IRowMeta m_incomingFieldsIncludedInOutputRowMeta;

  /**
   * The incoming row sets
   */
  protected List<IRowSet> m_incomingRowSets;

  /**
   * The list of processed row sets during the getRow of each incoming input stream
   */
  protected boolean[] m_finishedRowSets;

  /**
   * A collection of the frame buffers per input frame
   */
  protected List<List<Object[]>> m_frameBuffers = new ArrayList<List<Object[]>>();

  /**
   * Holds the row meta associated with each frame buffer or reservoir sampler
   */
  protected List<IRowMeta> m_infoMetas = new ArrayList<IRowMeta>();

  /**
   * A index used to reference a line for the incoming rows when we are processing row by row with
   * reservoir sampling active.
   */
  protected int m_rowByRowReservoirSampleIndex;

  /**
   * Batch size
   */
  protected int m_batchSize = 1000;

  /**
   * Reservoir Samplers size
   */
  protected int m_reservoirSamplersSize;

  /**
   * True if input stream values should be copied to the output stream
   */
  protected boolean m_includeInputAsOutput;

  /**
   * The script to run
   */
  protected String m_script;

  /**
   * Lookup for output indexes

   protected Map<String, Integer> m_outputMetaIndexLookup = new HashMap<String, Integer>(); */

  /**
   * Lookup output indexes for just input fields that are being copied to the output
   */
  protected Map<String, Integer> m_nonScriptOutputMetaIndexLookup = new HashMap<String, Integer>();

  /**
   * Variables to retrieve or columns present in pandas data frame that are not defined in the
   * output meta
   */
  protected List<String> m_varsOrColsNotDefinedInOutputMeta = new ArrayList<String>();

  /**
   * Variables or columns defined in the output meta that are not present in the variables to
   * retrieve or columns in the pandas data frame. Script logic (based on input values) could
   * dictate that some variables are not set or dataframe columns not generated for some reason.
   */
  protected List<String> m_varsOrColsInOutputMetaNotPresent = new ArrayList<String>();

  protected boolean m_first = true;

  /**
   * holds any unset variables for a script execution
   */
  protected List<String> m_unsetVars = new ArrayList<String>();

  protected Set<String> m_tmpSet = new HashSet<String>();

  protected static String loadScriptFromFile( String file ) throws HopException {
    FileObject scriptF = HopVfs.getFileObject( file );

    BufferedReader br = null;
    StringBuilder b = new StringBuilder();
    try {
      if ( !scriptF.exists() ) {
        throw new HopException(
            BaseMessages.getString( PKG, "RScriptExecutorData.Error.ScriptFileDoesNotExist", file ) );
      }

      InputStream is = HopVfs.getInputStream( scriptF );
      InputStreamReader isr = new InputStreamReader( is );
      br = new BufferedReader( isr );

      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        b.append( line ).append( "\n" );
      }

      br.close();
      br = null;
    } catch ( IOException e ) {
      throw new HopException( e );
    } finally {
      if ( br != null ) {
        try {
          br.close();
          br = null;
        } catch ( IOException e ) {
          throw new HopException( e );
        }
      }
    }

    return b.toString();
  }

  /**
   * Initialise a lookup on output indexes of any fields being copied from input to output. User may
   * re-order output fields in the dialog for this step, so we need the lookup.
   */
  public void initNonScriptOutputIndexLookup() {
    for ( IValueMeta v : m_incomingFieldsIncludedInOutputRowMeta.getValueMetaList() ) {
      int outIndex = m_outputRowMeta.indexOfValue( v.getName() );
      m_nonScriptOutputMetaIndexLookup.put( v.getName(), outIndex );
    }
  }
}
