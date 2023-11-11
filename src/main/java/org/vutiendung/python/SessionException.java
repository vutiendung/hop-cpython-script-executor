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

package org.phalanxdev.python;

import java.io.Serializable;

/**
 * Exception for problems stemming from python
 *
 * @author Mark Hall (mhall{[at]}waikato{[dot]}ac{[dot]}nz)
 */
public class SessionException extends Exception implements Serializable {

  /**
   * for serialization
   */
  static final long serialVersionUID = 5995231201785697655L;

  public SessionException() {
    super();
  }

  public SessionException( String message ) {
    super( message );
  }

  public SessionException( String message, Throwable cause ) {
    this( message );
    initCause( cause );
    fillInStackTrace();
  }

  public SessionException( Throwable cause ) {
    this( cause.getMessage(), cause );
  }
}
