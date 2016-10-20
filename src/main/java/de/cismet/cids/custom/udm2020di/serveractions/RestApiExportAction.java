/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;
import de.cismet.cids.custom.udm2020di.types.AggregationValuesBean;

import de.cismet.cids.server.actions.DownloadFileAction;
import de.cismet.cids.server.actions.ServerActionParameter;

import de.cismet.cidsx.base.types.MediaTypes;
import de.cismet.cidsx.base.types.Type;

import de.cismet.cidsx.server.actions.RestApiCidsServerAction;
import de.cismet.cidsx.server.api.types.ActionInfo;
import de.cismet.cidsx.server.api.types.ActionParameterInfo;
import de.cismet.cidsx.server.api.types.GenericResourceWithContentType;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@org.openide.util.lookup.ServiceProvider(service = RestApiCidsServerAction.class)
public class RestApiExportAction implements RestApiCidsServerAction {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger LOGGER = Logger.getLogger(RestApiExportAction.class);

    public static final String TASK_NAME = "restApiExportAction";

    //~ Instance fields --------------------------------------------------------

    protected final ActionInfo actionInfo;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new RestApiExportAction object.
     */
    public RestApiExportAction() {
        actionInfo = new ActionInfo();
        actionInfo.setName("REST API Export");
        actionInfo.setActionKey(TASK_NAME);
        actionInfo.setDescription("Exports Data from DWHs");

        final List<ActionParameterInfo> parameterDescriptions = new LinkedList<ActionParameterInfo>();
        final ActionParameterInfo parameterDescription = new ActionParameterInfo();
        parameterDescription.setKey("aggregationValues");
        parameterDescription.setType(Type.JAVA_CLASS);
        parameterDescription.setDescription("Aggregation Values");
        parameterDescription.setAdditionalTypeInfo(de.cismet.cids.custom.udm2020di.types.AggregationValuesBean.class
                    .getName());
        parameterDescriptions.add(parameterDescription);
        actionInfo.setParameterDescription(parameterDescriptions);

        final ActionParameterInfo bodyDescription = new ActionParameterInfo();
        bodyDescription.setKey("body");
        bodyDescription.setType(Type.STRING);
        bodyDescription.setMediaType("application/json");
        bodyDescription.setDescription("Deprecated body parameter, use server action parameter 'filename' instead!");
        actionInfo.setBodyDescription(bodyDescription);

        final ActionParameterInfo returnDescription = new ActionParameterInfo();
        returnDescription.setKey("return");
        returnDescription.setType(Type.BYTE);
        returnDescription.setMediaType(MediaTypes.APPLICATION_ZIP);
        returnDescription.setDescription("Returns the zipped output");
        actionInfo.setResultDescription(returnDescription);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public ActionInfo getActionInfo() {
        return this.actionInfo;
    }

    @Override
    public GenericResourceWithContentType execute(final Object body, final ServerActionParameter... params) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("executing '" + this.getTaskName() + "' with "
                        + params.length + " server action parameters and body object: " + (body != null));
        }

        AggregationValuesBean aggregationValuesBean;

        for (final ServerActionParameter sap : params) {
            if (sap != null) {
                final String paramKey = sap.getKey();
                final Object paramValue = sap.getValue();
                
                LOGGER.debug(paramKey + " : " + paramValue);

                /*if (paramKey.equalsIgnoreCase("aggregationValues")) {
                    if (paramValue instanceof AggregationValuesBean) {
                        aggregationValuesBean = (AggregationValuesBean)paramValue;
                    } else {
                        final String message = "unsupported type of action parameter '" + paramKey + "': "
                                    + paramValue.getClass().getSimpleName();
                        LOGGER.error(message);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(paramValue);
                        }
                        throw new RuntimeException(message);
                    }
                }*/
            }
        }

        /*if (aggregationValuesBean == null) {
            final String message = "missing mandatory action parameter 'aggregationValues'!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }*/
        
        LOGGER.debug(body);
        
        

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        try(final ZipOutputStream zipStream = new ZipOutputStream(output)) {
            zipStream.putNextEntry(new ZipEntry("aggregationValues"));
            /*zipStream.write(OracleExport.JSON_MAPPER.writerFor(AggregationValuesBean.class).writeValueAsBytes(
                    aggregationValuesBean));*/
            zipStream.write(body.toString().getBytes());
            zipStream.closeEntry();
        } catch (IOException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }

        final byte[] result = output.toByteArray();
        return new GenericResourceWithContentType(MediaTypes.APPLICATION_ZIP, result);
    }

    @Override
    public String getTaskName() {
        return TASK_NAME;
    }
}
