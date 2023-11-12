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

import org.apache.hop.core.Const;
import org.apache.hop.core.Props;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopPluginException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.PipelineMeta;
import org.apache.hop.pipeline.transform.BaseTransformMeta;
import org.apache.hop.pipeline.transform.ITransformDialog;
import org.apache.hop.pipeline.transform.stream.IStream;
import org.apache.hop.ui.core.dialog.ErrorDialog;
import org.apache.hop.ui.core.dialog.MessageBox;
import org.apache.hop.ui.core.dialog.ShowMessageDialog;
import org.apache.hop.ui.core.gui.GuiResource;
import org.apache.hop.ui.core.widget.ColumnInfo;
import org.apache.hop.ui.core.widget.ComboVar;
import org.apache.hop.ui.core.widget.StyledTextComp;
import org.apache.hop.ui.core.widget.TableView;
import org.apache.hop.ui.core.widget.TextVar;
import org.apache.hop.ui.pipeline.transform.BaseTransformDialog;
import org.apache.hop.ui.util.EnvironmentUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class CPythonScriptExecutorDialog extends BaseTransformDialog implements ITransformDialog {

  private static Class<?> PKG = CPythonScriptExecutorMeta.class;

  private CTabFolder wctfContainer;

  private CTabItem wctiConfig, wctiScript, wctiFields, wctiLibrary, wctiScriptEditor;
  private Composite wcConfig, wcScript, wcFields, wcLibrary, wcScriptEditor;
  private SelectionAdapter lsDef;

  /**
   * Configure tab
   */
  private Group wgRowHandling, wgOptions;
  //options group
  private Label  wlPythonCommand, wlPyPathEntries, wlPyServerID;
  private TextVar wtvPythonCommand, wtvPyPathEntries, wtvPyServerID;
  //table
  private TableView wtvInputFrames;

  /**
   * Script tab
   */
  private Label wlLoadScriptFile, wlScriptLocation, wlScript;
  private Button wbLoadScriptFile, wbScriptBrowse;
  private TextVar wtvScriptLocation;
  private StyledTextComp wstcScriptEditor;


  /**
   * Library tab
   */
  private Label wlLibrary;
  private StyledTextComp wstcLibraryEditor;

  /**
   * scriptEditor tab
   */
  private Label wlScriptEditor;
  private StyledTextComp wstcScriptEditor2;
  private Button saveFile;

  /**
   * Fields tab
   */
  private Label wlPyVarsToGet;
  private TextVar wtvPyVarsToGet;
  private Label wlOutputFields;
  private TableView wtvOutputFields;
  private Button wbGetFields;
  private Button wbIncludeRowIndex;

  private FormData fd;
  private Control lastControl;

  //constants
  private static final int FIRST_LABEL_RIGHT_PERCENTAGE = 35;
  private static final int FIRST_PROMPT_RIGHT_PERCENTAGE = 55;
  private static final int SECOND_LABEL_RIGHT_PERCENTAGE = 65;
  private static final int SECOND_PROMPT_RIGHT_PERCENTAGE = 80;
  private static final int MARGIN = Const.MARGIN;
  private static int MIDDLE;

  protected CPythonScriptExecutorMeta inputMeta;
  protected CPythonScriptExecutorMeta originalMeta;

  //listeners
  ModifyListener simpleModifyListener = new ModifyListener() {
    @Override public void modifyText( ModifyEvent e ) {
      inputMeta.setChanged();
    }
  };

  SelectionAdapter simpleSelectionAdapter = new SelectionAdapter() {
    @Override public void widgetDefaultSelected( SelectionEvent e ) {
      ok();
    }
  };

  public CPythonScriptExecutorDialog( Shell parent, IVariables variables, Object inMeta, PipelineMeta tr, String sname ) {
    super( parent, variables, (BaseTransformMeta) inMeta, tr, sname );

    inputMeta = (CPythonScriptExecutorMeta) inMeta;
    originalMeta = (CPythonScriptExecutorMeta) inputMeta.clone();
  }

  public CPythonScriptExecutorDialog(Shell parent, IVariables variables, BaseTransformMeta baseTransformMeta,
      PipelineMeta pipelineMeta, String transformName) {
    super(parent, variables, baseTransformMeta, pipelineMeta, transformName);
    inputMeta = (CPythonScriptExecutorMeta) baseTransformMeta;
    originalMeta = (CPythonScriptExecutorMeta) inputMeta.clone();
  }

  public CPythonScriptExecutorDialog(Shell parent, int nr, IVariables variables, Object in, PipelineMeta tr ) {
    super(parent, nr, variables, (BaseTransformMeta) in, tr);

    inputMeta = (CPythonScriptExecutorMeta) in;
    originalMeta = (CPythonScriptExecutorMeta) inputMeta.clone();
  }

  @Override public String open() {

    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, inputMeta );

    changed = inputMeta.hasChanged();

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setLayout( formLayout );
    shell.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.Shell.Title" ) ); //$NON-NLS-1$

    MIDDLE = props.getMiddlePct();

    // Stepname line
    wlTransformName = new Label( shell, SWT.RIGHT );
    wlTransformName
        .setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.Stepname.Label" ) ); //$NON-NLS-1$
    props.setLook( wlTransformName );
    fdlTransformName = new FormData();
    fdlTransformName.left = new FormAttachment( 0, 0 );
    fdlTransformName.right = new FormAttachment( MIDDLE, -MARGIN );
    fdlTransformName.top = new FormAttachment( 0, MARGIN );
    wlTransformName.setLayoutData( fdlTransformName );
    wTransformName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    wTransformName.setText( transformName );
    props.setLook( wTransformName );
    wTransformName.addModifyListener( simpleModifyListener );
    fdTransformName = new FormData();
    fdTransformName.left = new FormAttachment( MIDDLE, 0 );
    fdTransformName.top = new FormAttachment( 0, MARGIN );
    fdTransformName.right = new FormAttachment( 100, 0 );
    wTransformName.setLayoutData( fdTransformName );
    lastControl = wTransformName;

    wctfContainer = new CTabFolder( shell, SWT.BORDER );
    props.setLook( wctfContainer, Props.WIDGET_STYLE_TAB );
    if (!EnvironmentUtils.getInstance().isWeb()) {
      wctfContainer.setSimple(false);
    }

    addConfigureTab();
    addScriptTab();
    addFieldsTab();
    addExtraLibraryTab();
    if(inputMeta.getLoadScriptAtRuntime()) {
      addScriptEditorTab();
    }

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wTransformName, MARGIN );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 95, -50 );
    wctfContainer.setLayoutData( fd );

    //change text after change the file in script tab and click to any tab
    wctfContainer.addSelectionListener(new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        if(wctiScriptEditor != null && wtvScriptLocation.getText() != "") {
          wlScriptEditor.setText( "Editing file: " + wtvScriptLocation.getText() );

          //load file here
          try {
            wstcScriptEditor2.setText(readFileToString(wtvScriptLocation.getText()));
          } catch (Exception ex) {
            logError(ex.getMessage());
          }
        }
      }
    } );

    // some buttons
    wOk = new Button( shell, SWT.PUSH );
    wOk.setText( BaseMessages.getString( PKG, "System.Button.OK" ) ); //$NON-NLS-1$
    wOk.addListener( SWT.Selection, new Listener() {
      @Override public void handleEvent( Event e ) {
        ok();
      }
    } );
    wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) ); //$NON-NLS-1$
    wCancel.addListener( SWT.Selection, new Listener() {
      @Override public void handleEvent( Event e ) {
        cancel();
      }
    } );
    setButtonPositions( new Button[] { wOk, wCancel }, MARGIN, null );

    lsDef = new SelectionAdapter() {
      @Override public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };

    wTransformName.addSelectionListener( lsDef );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      @Override public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData( inputMeta );

    inputMeta.setChanged( changed );

    wctfContainer.setSelection( 0 );
    // Set the shell size, based upon previous time...
    setSize();

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return transformName;
  }

  private void addScriptEditorTab() {
    if(wbLoadScriptFile.getSelection() || inputMeta.getLoadScriptAtRuntime()) {

      if(wctiScriptEditor != null) {
        wctiScriptEditor.dispose();
      }
      
      wctiScriptEditor = new CTabItem( wctfContainer, SWT.NONE );
      wctiScriptEditor.setText( "Script editor" );

      wcScriptEditor = new Composite( wctfContainer, SWT.NONE );
      props.setLook( wcScriptEditor );
      FormLayout fl = new FormLayout();
      fl.marginWidth = 3;
      fl.marginHeight = 3;
      wcScriptEditor.setLayout( fl );

      wlScriptEditor = new Label( wcScriptEditor, SWT.LEFT );
      props.setLook( wlScriptEditor );
      wlScriptEditor.setText( "Editing file: " + inputMeta.m_loadScriptFile );
      fd = new FormData();
      fd.left = new FormAttachment( 0, 0 );
      fd.top = new FormAttachment( lastControl, MARGIN );
      wlScriptEditor.setLayoutData( fd );
      lastControl = wlScriptEditor;
  
      //Textbox to contains file content
      wstcScriptEditor2 =
          new StyledTextComp( variables, wcScriptEditor, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, false );
      props.setLook( wstcScriptEditor2, Props.WIDGET_STYLE_FIXED);
    
      fd = new FormData();
      fd.left = new FormAttachment( 0, 0 );
      fd.top = new FormAttachment( lastControl, MARGIN );
      fd.right = new FormAttachment( 100, -2 * MARGIN );
      fd.bottom = new FormAttachment( 95, -MARGIN );
      wstcScriptEditor2.setLayoutData( fd );
      lastControl = wstcScriptEditor2;

      Button btSaveFile = new Button( wcScriptEditor, SWT.PUSH );
      btSaveFile.setText( "Save file" ); 
      btSaveFile.addListener( SWT.Selection, new Listener() {
        @Override public void handleEvent( Event e ) {
          //save file
          try {
              writeStringToFile(wstcScriptEditor2.getText(), wtvScriptLocation.getText());

              //notify
              MessageBox messageBox = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.ABORT | SWT.RETRY | SWT.IGNORE);
              messageBox.setText("Information");
              messageBox.setMessage("File has been saved!");
              messageBox.open();

            } catch (HopException e1) {
              logError(e1.getMessage());
            }
        }
      } );


      fd = new FormData();
      fd.right = new FormAttachment( 100, -MARGIN );
      fd.top = new FormAttachment( lastControl, MARGIN );
      btSaveFile.setLayoutData(fd);
    
      wcScriptEditor.layout();
      wctiScriptEditor.setControl( wcScriptEditor );
    }
  }

  private void addConfigureTab() {
    wctiConfig = new CTabItem( wctfContainer, SWT.NONE );
    wctiConfig.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.ConfigTab.TabTitle" ) );

    wcConfig = new Composite( wctfContainer, SWT.NONE );
    props.setLook( wcConfig );
    FormLayout wflConfig = new FormLayout();
    wflConfig.marginWidth = 3;
    wflConfig.marginHeight = 3;
    wcConfig.setLayout( wflConfig );

    addOptionsGroup();

    // Input Frames Label
    Label inputFramesLab = new Label( wcConfig, SWT.RIGHT );
    inputFramesLab.setText( BaseMessages.getString( PKG, "CPythonScriptExecutor.InputFrames.Label" ) );
    props.setLook( inputFramesLab );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( wgOptions, MARGIN );
    inputFramesLab.setLayoutData( fd );
    lastControl = inputFramesLab;

    // table
    ColumnInfo[]
        colinf =
        new ColumnInfo[] {
            new ColumnInfo( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.FrameNames.StepName" ),
                ColumnInfo.COLUMN_TYPE_CCOMBO, false ),
            new ColumnInfo( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.FrameNames.FrameName" ),
                ColumnInfo.COLUMN_TYPE_TEXT, false ) };

    String[] previousSteps = pipelineMeta.getPrevTransformNames( transformName );
    if ( previousSteps != null ) {
      colinf[0].setComboValues( previousSteps );
    }

    wtvInputFrames =
        new TableView( variables, wcConfig, SWT.FULL_SELECTION | SWT.MULTI, colinf, 1, simpleModifyListener, props );
    fd = new FormData();
    fd.top = new FormAttachment( lastControl, MARGIN );
    fd.bottom = new FormAttachment( 100, -MARGIN * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    wtvInputFrames.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, -MARGIN * 2 );
    fd.bottom = new FormAttachment( 100, 0 );
    wcConfig.setLayoutData( fd );

    wcConfig.layout();
    wctiConfig.setControl( wcConfig );
  }

  private void addScriptTab() {
    wctiScript = new CTabItem( wctfContainer, SWT.NONE );
    wctiScript.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.ScriptTab.TabTitle" ) ); //$NON-NLS-1$
    wcScript = new Composite( wctfContainer, SWT.NONE );
    props.setLook( wcScript );
    FormLayout scriptLayout = new FormLayout();
    scriptLayout.marginWidth = 3;
    scriptLayout.marginHeight = 3;
    wcScript.setLayout( scriptLayout );

    wlLoadScriptFile = new Label( wcScript, SWT.RIGHT );
    props.setLook( wlLoadScriptFile );
    wlLoadScriptFile
        .setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.LoadScript.Label" ) ); //$NON-NLS-1$
    wlLoadScriptFile.setLayoutData( getFirstLabelFormData() );

    wbLoadScriptFile = new Button( wcScript, SWT.CHECK );
    props.setLook( wbLoadScriptFile );
    FormData fd = getFirstPromptFormData( wlLoadScriptFile );
    fd.right = null;
    wbLoadScriptFile.setLayoutData( fd );
    wbLoadScriptFile.setToolTipText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.LoadScript.TipText" ) );
    lastControl = wbLoadScriptFile;

    wbLoadScriptFile.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        checkWidgets();
      }
    } );

    wlScriptLocation = new Label( wcScript, SWT.RIGHT );
    props.setLook( wlScriptLocation );
    wlScriptLocation.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.ScriptFile.Label" ) );
    wlScriptLocation.setLayoutData( getFirstLabelFormData() );

    wbScriptBrowse = new Button( wcScript, SWT.PUSH | SWT.CENTER );
    wbScriptBrowse.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.Browse.Button" ) );
    props.setLook( wbScriptBrowse );
    fd = new FormData();
    fd.right = new FormAttachment( 100, -MARGIN );
    fd.top = new FormAttachment( lastControl, MARGIN );
    wbScriptBrowse.setLayoutData( fd );

    wbScriptBrowse.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent e ) {
        FileDialog dialog = new FileDialog( shell, SWT.OPEN );

        if ( !org.apache.hop.core.util.Utils.isEmpty( wtvScriptLocation.getText() ) ) {
          dialog.setFileName( variables.resolve( wtvScriptLocation.getText() ) );
        }

        if ( dialog.open() != null ) {
          wtvScriptLocation
              .setText( dialog.getFilterPath() + System.getProperty( "file.separator" ) + dialog.getFileName() );
        }
      }
    } );

    wtvScriptLocation = new TextVar( variables, wcScript, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wtvScriptLocation );
    fd = new FormData();
    fd.left = new FormAttachment( wlScriptLocation, MARGIN );
    fd.top = new FormAttachment( lastControl, MARGIN );
    fd.right = new FormAttachment( wbScriptBrowse, -MARGIN );
    wtvScriptLocation.setLayoutData( fd );
    lastControl = wtvScriptLocation;

    wlScript = new Label( wcScript, SWT.LEFT );
    props.setLook( wlScript );
    wlScript.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.ManualScript.Label" ) );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, MARGIN );
    wlScript.setLayoutData( fd );
    lastControl = wlScript;

    wstcScriptEditor =
        new StyledTextComp( variables, wcScript, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL,
            true, false );
    props.setLook( wstcScriptEditor, Props.WIDGET_STYLE_FIXED );

    wlPyVarsToGet = new Label( wcScript, SWT.RIGHT );
    props.setLook( wlPyVarsToGet );
    wlPyVarsToGet.setText( "Python Variables to Get:" );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( FIRST_LABEL_RIGHT_PERCENTAGE, 0 );
    fd.bottom = new FormAttachment( 100, -MARGIN * 2 );
    wlPyVarsToGet.setLayoutData( fd );

    wtvPyVarsToGet = new TextVar( variables, wcScript, SWT.SINGLE | SWT.LEAD | SWT.BORDER );
    props.setLook( wtvPyVarsToGet );
    fd = new FormData();
    fd.left = new FormAttachment( wlPyVarsToGet, MARGIN );
    fd.right = new FormAttachment( SECOND_PROMPT_RIGHT_PERCENTAGE, 0 );
    fd.bottom = new FormAttachment( 100, -MARGIN * 2 );
    wtvPyVarsToGet.setLayoutData( fd );
    wtvPyVarsToGet.addFocusListener( new FocusAdapter() {
      @Override public void focusLost( FocusEvent e ) {
        super.focusLost( e );
        String currVars = wtvPyVarsToGet.getText();
        if ( !org.apache.hop.core.util.Utils.isEmpty( currVars ) ) {
          List<String> varList = stringToList( currVars );
          wbGetFields.setEnabled( varList.size() == 1 );
          wbIncludeRowIndex.setEnabled( varList.size() == 1 );
        }
      }
    } );

    wstcScriptEditor.addModifyListener( simpleModifyListener );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, MARGIN );
    fd.right = new FormAttachment( 100, -2 * MARGIN );
    fd.bottom = new FormAttachment( wtvPyVarsToGet, -MARGIN );
    wstcScriptEditor.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( wtvPyVarsToGet, 0 );
    wcScript.setLayoutData( fd );

    wcScript.layout();
    wctiScript.setControl( wcScript );
  }

  private void addExtraLibraryTab() {
    wctiLibrary = new CTabItem( wctfContainer, SWT.NONE );
    wctiLibrary.setText( "Python Library" ); //$NON-NLS-1$
    wcLibrary = new Composite( wctfContainer, SWT.NONE );
    props.setLook( wcLibrary );
    FormLayout libraryLayout = new FormLayout();
    libraryLayout.marginWidth = 3;
    libraryLayout.marginHeight = 3;
    wcLibrary.setLayout( libraryLayout );
  
    wlLibrary = new Label( wcLibrary, SWT.LEFT );
    props.setLook( wlLibrary );
    wlLibrary.setText( "Extra Library" );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, MARGIN );
    wlLibrary.setLayoutData( fd );
    lastControl = wlLibrary;
  
    //Textbox to contains library
    wstcLibraryEditor =
        new StyledTextComp( variables, wcLibrary, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, false );
    props.setLook( wstcLibraryEditor, Props.WIDGET_STYLE_FIXED);
  
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( lastControl, MARGIN );
    fd.right = new FormAttachment( 100, -2 * MARGIN );
    fd.bottom = new FormAttachment( 100, -MARGIN );
    wstcLibraryEditor.setLayoutData( fd );
  
    wcLibrary.layout();
    wctiLibrary.setControl( wcLibrary );
  }

  private void addFieldsTab() {
    // --- fields tab
    wctiFields = new CTabItem( wctfContainer, SWT.NONE );
    wctiFields.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.FieldsTab.TabTitle" ) ); //$NON-NLS-1$
    wcFields = new Composite( wctfContainer, SWT.NONE );
    props.setLook( wcFields );
    FormLayout fieldsLayout = new FormLayout();
    fieldsLayout.marginWidth = 3;
    fieldsLayout.marginHeight = 3;
    wcFields.setLayout( fieldsLayout );

    wlOutputFields = new Label( wcFields, SWT.LEFT );
    wlOutputFields
        .setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.OutFields.Label" ) ); //$NON-NLS-1$
    props.setLook( wlOutputFields );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( MIDDLE, -MARGIN );
    fd.top = new FormAttachment( 0, MARGIN );
    wlOutputFields.setLayoutData( fd );
    lastControl = wlOutputFields;

    // table
    ColumnInfo[]
        colinf2 =
        new ColumnInfo[] { new ColumnInfo( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.OutFields.Name" ),
            ColumnInfo.COLUMN_TYPE_TEXT, false ),
            new ColumnInfo( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.OutFields.Type" ),
                ColumnInfo.COLUMN_TYPE_CCOMBO, /*ValueMeta.getAllTypes()*/ ValueMetaFactory.getAllValueMetaNames(),
                false ) };

    wtvOutputFields =
        new TableView( variables, wcFields, SWT.FULL_SELECTION | SWT.MULTI, colinf2, 1, simpleModifyListener,
            props );
    fd = new FormData();
    fd.top = new FormAttachment( lastControl, MARGIN * 2 );
    fd.bottom = new FormAttachment( 100, -MARGIN * 2 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    wtvOutputFields.setLayoutData( fd );

    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.top = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.bottom = new FormAttachment( 100, 0 );
    wcFields.setLayoutData( fd );

    wcFields.layout();
    wctiFields.setControl( wcFields );
  }

  private void getFrameFields( CPythonScriptExecutorMeta meta ) {

    try {
      meta.setOutputFields( new RowMeta() );
      List<String> frameNames = meta.getFrameNames();
      List<IStream> infoStreams = meta.getStepIOMeta().getInfoStreams();
      List<IRowMeta> incomingMetas = new ArrayList<IRowMeta>();
      if ( frameNames.size() > 0 && infoStreams.size() > 0 ) {

        for ( int i = 0; i < infoStreams.size(); i++ ) {
          incomingMetas.add( pipelineMeta.getTransformFields( variables, infoStreams.get( i ).getTransformMeta() ) );
        }
      }

      ShowMessageDialog
          smd =
          new ShowMessageDialog( this.getParent(), SWT.YES | SWT.NO | SWT.ICON_WARNING,
              BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.GetFields.Dialog.Title" ),
              BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.GetFields.Dialog.Message" ), false );
      int buttonID = smd.open();

      if ( buttonID == SWT.YES ) {
        IRowMeta rowMeta = new RowMeta();
        meta.getFields( rowMeta, "bogus", incomingMetas.toArray( new IRowMeta[incomingMetas.size()] ), null,
            variables, null );

        wtvOutputFields.clearAll();
        for ( int i = 0; i < rowMeta.size(); i++ ) {
          TableItem item = new TableItem( wtvOutputFields.table, SWT.NONE );
          item.setText( 1, Const.NVL( rowMeta.getValueMeta( i ).getName(), "" ) );
          item.setText( 2, Const.NVL( rowMeta.getValueMeta( i ).getTypeDesc(), "" ) );
        }
        wtvOutputFields.removeEmptyRows();
        wtvOutputFields.setRowNums();
        wtvOutputFields.optWidth( true );
      }
    } catch ( HopException ex ) {
      new ErrorDialog( shell, transformName,
          BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.ErrorGettingFields" ), ex );
    }
  }

  private void addOptionsGroup() {
    // add second group
    wgOptions = new Group( wcConfig, SWT.SHADOW_NONE );
    props.setLook( wgOptions );
    wgOptions.setText( BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.ConfigTab.OptionsGroup" ) );
    FormLayout optionsGroupLayout = new FormLayout();
    optionsGroupLayout.marginWidth = 10;
    optionsGroupLayout.marginHeight = 10;
    wgOptions.setLayout( optionsGroupLayout );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( wgRowHandling, MARGIN );
    wgOptions.setLayoutData( fd );
    addPythonOptions();
  }

  private void addPythonOptions() {
    wlPythonCommand = new Label( wgOptions, SWT.RIGHT );
    wlPythonCommand.setText( BaseMessages.getString( PKG, "CPythonScriptExecutor.PythonCommand.Label" ) );
    wlPythonCommand.setToolTipText( BaseMessages.getString( PKG, "CPythonScriptExecutor.PythonCommand.TipText" ) );
    props.setLook( wlPythonCommand );
    wlPythonCommand.setLayoutData( getFirstLabelFormData() );

    wtvPythonCommand = new TextVar( variables, wgOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wtvPythonCommand );
    FormData fd = getFirstPromptFormData( wlPythonCommand );
    fd.right = new FormAttachment( 95, 0 );
    wtvPythonCommand.setLayoutData( fd );
    lastControl = wtvPythonCommand;

    wlPyPathEntries = new Label( wgOptions, SWT.RIGHT );
    wlPyPathEntries.setText( BaseMessages.getString( PKG, "CPythonScriptExecutor.PyPathEntries.Label" ) );
    wlPyPathEntries.setToolTipText( BaseMessages.getString( PKG, "CPythonScriptExecutor.PyPathEntries.TipText" ) );
    props.setLook( wlPyPathEntries );
    wlPyPathEntries.setLayoutData( getFirstLabelFormData() );

    wtvPyPathEntries = new TextVar( variables, wgOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wtvPyPathEntries );
    fd = getFirstPromptFormData( wlPyPathEntries );
    fd.right = new FormAttachment( 95, 0 );
    wtvPyPathEntries.setLayoutData( fd );
    lastControl = wtvPyPathEntries;

    wlPyServerID = new Label( wgOptions, SWT.RIGHT );
    wlPyServerID.setText( BaseMessages.getString( PKG, "CPythonScriptExecutor.PyServerID.Label" ) );
    wlPyServerID.setToolTipText( BaseMessages.getString( PKG, "CPythonScriptExecutor.PyServerID.TipText" ) );
    props.setLook( wlPyServerID );
    wlPyServerID.setLayoutData( getFirstLabelFormData() );

    wtvPyServerID = new TextVar( variables, wgOptions, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wtvPyServerID );
    fd = getFirstPromptFormData( wlPyServerID );
    fd.right = new FormAttachment( 95, 0 );
    wtvPyServerID.setLayoutData( fd );
    lastControl = wtvPyServerID;
  }

  protected void getData( CPythonScriptExecutorMeta meta ) {
    setItemText( wtvPyVarsToGet, listToString( meta.getPythonVariablesToGet() ) );
    setItemText( wtvPythonCommand, meta.getPythonCommand() );
    setItemText( wtvPyPathEntries, meta.getPyPathEntries() );
    wstcScriptEditor.setText( meta.getScript() == null ? "" : meta.getScript() ); //$NON-NLS-1$
    wstcLibraryEditor.setText(meta.getLibrary() == null ? "": meta.getLibrary());
    wbLoadScriptFile.setSelection( meta.getLoadScriptAtRuntime() );
    setItemText( wtvScriptLocation, meta.getScriptToLoad() );

    setInputToFramesTableFields( meta );
    setOutputFieldsTableFields( meta );

    checkWidgets();
  }

  private void varsToTableFields( CPythonScriptExecutorMeta meta ) {
    // List<IRowMeta> incomingMetas;
    IRowMeta incomingMetas = new RowMeta();
    if ( meta.getIncludeInputAsOutput() ) {
      List<String> frameNames = meta.getFrameNames();
      List<IStream> infoStreams = meta.getStepIOMeta().getInfoStreams();
      if ( frameNames.size() > 0 && infoStreams.size() > 0 ) {
        // incomingMetas = new ArrayList<IRowMeta>();

        try {
          for ( int i = 0; i < infoStreams.size(); i++ ) {
            incomingMetas.addRowMeta( pipelineMeta.getTransformFields( variables, infoStreams.get( i ).getTransformMeta() ) );
          }
        } catch ( HopException e ) {
          new ErrorDialog( shell, transformName,
              BaseMessages.getString( PKG, "CPythonScriptExecutorDialog.ErrorGettingFields" ), e );
          return;
        }
      }
    }

    wtvOutputFields.clearAll();
    if ( incomingMetas.size() > 0 ) {
      for ( IValueMeta vm : incomingMetas.getValueMetaList() ) {
        TableItem item = new TableItem( wtvOutputFields.table, SWT.NONE );
        item.setText( 1, Const.NVL( vm.getName(), "" ) );
        item.setText( 2, Const.NVL( vm.getTypeDesc(), "" ) );
      }
    }
    String vars = wtvPyVarsToGet.getText();
    if ( !org.apache.hop.core.util.Utils.isEmpty( vars ) ) {
      String[] vA = vars.split( "," );
      if ( vA.length > 0 ) {
        for ( String var : vA ) {
          TableItem item = new TableItem( wtvOutputFields.table, SWT.NONE );
          item.setText( 1, Const.NVL( var.trim(), "" ) );
          item.setText( 2, "String" );
        }
        wtvOutputFields.removeEmptyRows();
        wtvOutputFields.setRowNums();
        wtvOutputFields.optWidth( true );
      }
    }
  }

  private List<String> stringToList( String list ) {
    List<String> result = new ArrayList<String>();
    for ( String s : list.split( "," ) ) {
      if ( !org.apache.hop.core.util.Utils.isEmpty( s.trim() ) ) {
        result.add( s.trim() );
      }
    }

    return result;
  }

  private String listToString( List<String> list ) {
    StringBuilder b = new StringBuilder();
    for ( String s : list ) {
      b.append( s ).append( "," );
    }
    if ( b.length() > 0 ) {
      b.setLength( b.length() - 1 );
    }

    return b.toString();
  }

  private void setData( CPythonScriptExecutorMeta meta ) {
    meta.setPythonVariablesToGet( stringToList( wtvPyVarsToGet.getText() ) );
    meta.setPythonCommand( wtvPythonCommand.getText() );
    meta.setPyPathEntries( wtvPyPathEntries.getText() );
    meta.setScript( wstcScriptEditor.getText() );
    meta.setLibrary(wstcLibraryEditor.getText());
    meta.setLoadScriptAtRuntime( wbLoadScriptFile.getSelection() );
    meta.setScriptToLoad( wtvScriptLocation.getText() );

    // incoming stream/frame name data from table
    int numNonEmpty = wtvInputFrames.nrNonEmpty();
    List<String> frameNames = new ArrayList<String>();
    List<String> stepNames = new ArrayList<String>();
    meta.clearStepIOMeta();
    for ( int i = 0; i < numNonEmpty; i++ ) {
      TableItem item = wtvInputFrames.getNonEmpty( i );
      String stepName = item.getText( 1 ).trim();
      String frameName = item.getText( 2 ).trim();
      if ( !org.apache.hop.core.util.Utils.isEmpty( stepName ) ) {
        if ( org.apache.hop.core.util.Utils.isEmpty( frameName ) ) {
          frameName = CPythonScriptExecutorMeta.DEFAULT_FRAME_NAME_PREFIX + i;
        }
        frameNames.add( frameName );
        stepNames.add( stepName );
      }
    }

    meta.setFrameNames( frameNames );
    List<IStream> infoStreams = meta.getStepIOMeta().getInfoStreams();
    for ( int i = 0; i < infoStreams.size(); i++ ) {
      infoStreams.get( i ).setSubject( stepNames.get( i ) );
    }

    // output field data from table
    numNonEmpty = wtvOutputFields.nrNonEmpty();
    IRowMeta outRM = numNonEmpty > 0 ? new RowMeta() : null;
    for ( int i = 0; i < numNonEmpty; i++ ) {
      TableItem item = wtvOutputFields.getNonEmpty( i );
      String name = item.getText( 1 ).trim();
      String type = item.getText( 2 ).trim();
      if ( !org.apache.hop.core.util.Utils.isEmpty( name ) && !org.apache.hop.core.util.Utils.isEmpty( type ) ) {
        //IValueMeta vm = new ValueMeta( name, ValueMeta.getType( type ) );
        IValueMeta vm;
        try {
          vm = ValueMetaFactory.createValueMeta( name, ValueMetaFactory.getIdForValueMeta( type ) );
          outRM.addValueMeta( vm );
        } catch ( HopPluginException e ) {
          e.printStackTrace();
        }
      }
    }

    meta.setOutputFields( outRM );
  }

  protected void checkWidgets() {
    wtvScriptLocation.setEnabled( wbLoadScriptFile.getSelection() );
    wstcScriptEditor.setEnabled( !wbLoadScriptFile.getSelection() );
    if ( wbLoadScriptFile.getSelection() ) {
      wtvScriptLocation.setEditable( true );
      wstcScriptEditor.getTextWidget().setBackground( GuiResource.getInstance().getColorDemoGray() );
      addScriptEditorTab();
    } else {
      wtvScriptLocation.setEditable( false );
      props.setLook(wstcScriptEditor, Props.WIDGET_STYLE_FIXED);

      //remove tab editor
      if (wctiScriptEditor != null) {
        wctiScriptEditor.dispose();
      }
    }
    wbScriptBrowse.setEnabled( wbLoadScriptFile.getSelection() );

    String currVars = wtvPyVarsToGet.getText();
    if ( !org.apache.hop.core.util.Utils.isEmpty( currVars ) ) {
      List<String> varList = stringToList( currVars );
      //wbIncludeRowIndex.setEnabled( varList.size() == 1 );
    }
  }

  protected void setInputToFramesTableFields( CPythonScriptExecutorMeta meta ) {
    List<String> frameNames = meta.getFrameNames();
    // List<IStream> infoStreams = meta.getStepIOMeta().getInfoStreams();
    List<IStream> infoStreams = meta.getStepIOMeta().getInfoStreams();

    wtvInputFrames.clearAll();
    for ( int i = 0; i < infoStreams.size(); i++ ) {
      if ( infoStreams.get( i ).getSubject() != null ) {
        String stepName = infoStreams.get( i ).getSubject().toString();
        String frameName = frameNames.get( i );

        TableItem item = new TableItem( wtvInputFrames.table, SWT.NONE );
        item.setText( 1, Const.NVL( stepName, "" ) ); //$NON-NLS-1$
        item.setText( 2, Const.NVL( frameName, "" ) ); //$NON-NLS-1$

        // TransformMeta m = pipelineMeta.findTransform(stepName);
        // infoStreams.get(i).setTransformMeta(m);
      }
    }

    wtvInputFrames.removeEmptyRows();
    wtvInputFrames.setRowNums();
    wtvInputFrames.optWidth( true );
  }

  protected void setOutputFieldsTableFields( CPythonScriptExecutorMeta meta ) {
    IRowMeta outFields = meta.getOutputFields();

    if ( outFields != null && outFields.size() > 0 ) {
      for ( int i = 0; i < outFields.size(); i++ ) {
        IValueMeta vm = outFields.getValueMeta( i );
        String name = vm.getName();
        String type = vm.getTypeDesc();

        TableItem item = new TableItem( wtvOutputFields.table, SWT.NONE );
        item.setText( 1, Const.NVL( name, "" ) ); //$NON-NLS-1$
        item.setText( 2, Const.NVL( type, "" ) ); //$NON-NLS-1$
      }

      wtvOutputFields.removeEmptyRows();
      wtvOutputFields.setRowNums();
      wtvOutputFields.optWidth( true );
    }
  }

  private void cancel() {
    transformName = null;
    inputMeta.setChanged( changed );
    dispose();
  }

  /**
   * Ok general method
   */
  private void ok() {
    if ( org.apache.hop.core.util.Utils.isEmpty( wTransformName.getText() ) ) {
      return;
    }

    transformName = wTransformName.getText(); // return value

    setData( inputMeta );
    if ( !originalMeta.equals( inputMeta ) ) {
      inputMeta.setChanged();
      changed = inputMeta.hasChanged();
    }

    dispose();
  }

  private FormData getFirstLabelFormData() {
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( FIRST_LABEL_RIGHT_PERCENTAGE, 0 );
    fd.top = new FormAttachment( lastControl, MARGIN );
    return fd;
  }

  private FormData getSecondLabelFormData( Control prevControl ) {
    FormData fd = new FormData();
    fd.left = new FormAttachment( prevControl, 0 );
    fd.right = new FormAttachment( SECOND_LABEL_RIGHT_PERCENTAGE, 0 );
    fd.top = new FormAttachment( lastControl, MARGIN );
    return fd;
  }

  private FormData getFirstPromptFormData( Control prevControl ) {
    FormData fd = new FormData();
    fd.left = new FormAttachment( prevControl, MARGIN );
    fd.right = new FormAttachment( FIRST_PROMPT_RIGHT_PERCENTAGE, 0 );
    fd.top = new FormAttachment( lastControl, MARGIN );
    return fd;
  }

  private FormData getSecondPromptFormData( Control prevControl ) {
    FormData fd = new FormData();
    fd.left = new FormAttachment( prevControl, MARGIN );
    fd.top = new FormAttachment( lastControl, MARGIN );
    fd.right = new FormAttachment( SECOND_PROMPT_RIGHT_PERCENTAGE, 0 );
    return fd;
  }

  /**
   * SWT TextVar's throw IllegalArgumentExceptions for null.  Defend against this.
   *
   * @param item  - A TextVar, but could be generalized as needed.
   * @param value - Value to set
   */
  private void setItemText( TextVar item, String value ) {
    item.setText( value == null ? "" : value );
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
}