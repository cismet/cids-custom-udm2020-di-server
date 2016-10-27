/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.rest;

import com.fasterxml.jackson.databind.ObjectReader;


import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;
import de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction;
import de.cismet.cids.custom.udm2020di.serveractions.boris.BorisExportAction;
import de.cismet.cids.custom.udm2020di.serveractions.eprtr.EprtrExportAction;
import de.cismet.cids.custom.udm2020di.serveractions.moss.MossExportAction;
import de.cismet.cids.custom.udm2020di.serveractions.wa.WaExportAction;
import de.cismet.cids.custom.udm2020di.serveractions.wa.WagwExportAction;
import de.cismet.cids.custom.udm2020di.serveractions.wa.WaowExportAction;
import de.cismet.cids.custom.udm2020di.types.Parameter;
import de.cismet.cids.custom.udm2020di.types.rest.ExportOptions;
import de.cismet.cids.custom.udm2020di.types.rest.ExportTheme;

import de.cismet.cids.server.actions.ServerActionParameter;

import de.cismet.cidsx.base.types.MediaTypes;
import de.cismet.cidsx.base.types.Type;

import de.cismet.cidsx.server.actions.RestApiCidsServerAction;
import de.cismet.cidsx.server.api.types.ActionInfo;
import de.cismet.cidsx.server.api.types.ActionParameterInfo;
import de.cismet.cidsx.server.api.types.GenericResourceWithContentType;

import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_EXPORTFORMAT;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_INTERNAL;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_NAME;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_PARAMETER;
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@org.openide.util.lookup.ServiceProvider(service = RestApiCidsServerAction.class)
public class RestApiExportAction implements RestApiCidsServerAction {

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger LOGGER = Logger.getLogger(RestApiExportAction.class);

    private static final ObjectReader EXPORT_OPTIONS_READER = OracleExport.JSON_MAPPER.readerFor(ExportOptions.class);

    public static final String TASK_NAME = "restApiExportAction";

    public static final String PARAM_EXPORT_OPTIONS = "exportOptions";
    // public static final String PARAM_MERGE_DATASOURCE = "isMergeExternalDatasource";

    public static final HashMap<String, String> EXPORT_FORMATS = new HashMap<String, String>();
    public static final HashMap<String, String> CONTENT_TYPES = new HashMap<String, String>();

    static {
        EXPORT_FORMATS.put("csv", AbstractExportAction.PARAM_EXPORTFORMAT_CSV);
        EXPORT_FORMATS.put("xls", AbstractExportAction.PARAM_EXPORTFORMAT_XLS);
        EXPORT_FORMATS.put("xlsx", AbstractExportAction.PARAM_EXPORTFORMAT_XLSX);
        EXPORT_FORMATS.put("shp", AbstractExportAction.PARAM_EXPORTFORMAT_SHP);

        CONTENT_TYPES.put(AbstractExportAction.PARAM_EXPORTFORMAT_CSV, MediaTypes.TEXT_CSV + "; charset=UTF-8");
        CONTENT_TYPES.put(AbstractExportAction.PARAM_EXPORTFORMAT_XLS, "application/vnd.ms-excel");
        CONTENT_TYPES.put(
            AbstractExportAction.PARAM_EXPORTFORMAT_XLSX,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        CONTENT_TYPES.put(AbstractExportAction.PARAM_EXPORTFORMAT_SHP, MediaTypes.APPLICATION_ZIP);
    }

    //~ Instance fields --------------------------------------------------------

    private final ActionInfo actionInfo;

    private final HashMap<String, AbstractExportAction> exportActions = new HashMap<String, AbstractExportAction>();

    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new RestApiExportAction object.
     */
    public RestApiExportAction() {
        actionInfo = new ActionInfo();
        actionInfo.setName("REST API Export Action");
        actionInfo.setActionKey(TASK_NAME);
        actionInfo.setDescription("Exports Data from DWHs and optionally merges data with ShapeFiles");

        final List<ActionParameterInfo> parameterDescriptions = new LinkedList<ActionParameterInfo>();
        final ActionParameterInfo parameterDescription = new ActionParameterInfo();
        parameterDescription.setKey(PARAM_EXPORT_OPTIONS);
        parameterDescription.setType(Type.STRING);
        parameterDescription.setDescription(
            "de.cismet.cids.custom.udm2020di.types.rest.ExportOptions as plain **JSON**");
        parameterDescription.setAdditionalTypeInfo(ExportTheme.class.getName());
        parameterDescription.setMediaType("application/json");
        parameterDescription.setArray(true);
        parameterDescriptions.add(parameterDescription);

        final ActionParameterInfo bodyDescription = new ActionParameterInfo();
        bodyDescription.setKey("datasource");
        bodyDescription.setType(Type.STRING);
        bodyDescription.setMediaType(MediaTypes.APPLICATION_ZIP);
        bodyDescription.setDescription("Optional Datasource (zipped GeoJson Data)");
        actionInfo.setBodyDescription(bodyDescription);

        final ActionParameterInfo returnDescription = new ActionParameterInfo();
        returnDescription.setKey("return");
        returnDescription.setType(Type.BYTE);
        returnDescription.setMediaType(MediaTypes.APPLICATION_ZIP);
        returnDescription.setDescription("Returns the zipped export files");
        actionInfo.setResultDescription(returnDescription);

        LOGGER.info("new RestApiExportAction created");
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public ActionInfo getActionInfo() {
        return this.actionInfo;
    }

    @Override
    public GenericResourceWithContentType execute(final Object body, final ServerActionParameter... params)
            throws RuntimeException {
        LOGGER.info("executing '" + this.getTaskName() + "' with "
                    + params.length + " server action parameters and body object: " + (body != null));

        synchronized (this.exportActions) {
            if (this.exportActions.isEmpty()) {
                LOGGER.info("bootstrapping export actions ...");
                try {
                    exportActions.put("BORIS_SITE", new BorisExportAction());
                    exportActions.put("WAGW_STATION", new WagwExportAction());
                    exportActions.put("WAOW_STATION", new WaowExportAction());
                    exportActions.put("EPRTR_INSTALLATION", new EprtrExportAction());
                    exportActions.put("MOSS", new MossExportAction());
                } catch (Throwable ex) {
                    final String message = "could not execute '" + this.getTaskName()
                                + "': could not instantiate Leagcy Export Action: " + ex.getMessage();
                    LOGGER.fatal(message);
                    throw new RuntimeException(message, ex);
                }
            }
        }

        final long current = System.currentTimeMillis();
        final ServerActionParameter exportOptionsParameter;
        final String exportOptionsJson;
        final ExportOptions exportOptions;
        final LinkedHashMap<ExportTheme, Future<Object>> exportResults =
            new LinkedHashMap<ExportTheme, Future<Object>>();

        if (params.length == 0) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': no Server Action Parameters provided!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        } else if (params.length != 1) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': wrong number of Server Action Parameters provided: " + params.length;
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        // check PARAM_EXPORT_OPTIONS
        exportOptionsParameter = params[0];
        if ((exportOptionsParameter == null)
                    || !PARAM_EXPORT_OPTIONS.equalsIgnoreCase(exportOptionsParameter.getKey())) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': required Server Action Parameter '" + PARAM_EXPORT_OPTIONS + "' is missing";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        if ((exportOptionsParameter.getValue() == null)
                    || !(exportOptionsParameter.getValue() instanceof String)
                    || ((String)exportOptionsParameter.getValue()).isEmpty()) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': value of Server Action Parameter '" + PARAM_EXPORT_OPTIONS
                        + "' is no java.lang.String or empty";
            LOGGER.error(message);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(exportOptionsParameter.getValue());
            }
            throw new RuntimeException(message);
        }

        // deserialize EXPORT OPTIONS
        exportOptionsJson = (String)exportOptionsParameter.getValue();
        /*if (LOGGER.isDebugEnabled()) {
         *  LOGGER.debug(exportOptionsJson);}*/
        try {
            exportOptions = (ExportOptions)EXPORT_OPTIONS_READER.readValue(exportOptionsJson);
        } catch (IOException ex) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': could not deserialize '" + PARAM_EXPORT_OPTIONS + "' from JSON: " + ex.getMessage();
            LOGGER.error(message, ex);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(exportOptionsJson);
            }
            throw new RuntimeException(message, ex);
        }

        // check EXPORT THEMES
        if (exportOptions.getExportThemes().isEmpty()) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': no export themes found in Server Action Parameter '" + PARAM_EXPORT_OPTIONS + "'!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        // CHECK MERGE DATASOURCE
        // FIXME: if server-side global datasources are used, the body parameter can be null!
        if ((exportOptions.isMergeExternalDatasource() == true) && (body == null)) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': merge datasource is true but body parameter (ZIPPED GeoJson FILE) is empty!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        if ((exportOptions.isMergeExternalDatasource() == true) && !(body instanceof byte[])) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': merge datasource is true but body parameter (ZIPPED GeoJson FILE) is not of type byte[]!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        // CHECK EXPORT FORMAT
        if (!EXPORT_FORMATS.containsKey(exportOptions.getExportFormat())) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': unsupported export format '" + exportOptions.getExportFormat() + "'!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        if (exportOptions.isMergeExternalDatasource() && !"shp".equalsIgnoreCase(exportOptions.getExportFormat())) {
            LOGGER.warn("Requested export format is '" + EXPORT_FORMATS.get(exportOptions.getExportFormat())
                        + "' but merging with external datasources requires legacy export to '"
                        + EXPORT_FORMATS.get("shp") + "'!");
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("perfoming export to '" + EXPORT_FORMATS.get(exportOptions.getExportFormat())
                        + "' for " + exportOptions.getExportThemes().size()
                        + " themes and merge with external datasource: "
                        + exportOptions.isMergeExternalDatasource());
        }

        if (exportOptions.isMergeExternalDatasource() == true) {
            final ByteArrayInputStream bodyInputStream = new ByteArrayInputStream((byte[])body);
            try(final ZipInputStream zipInputStream = new ZipInputStream(bodyInputStream)) {
                final ZipEntry zipEntry = zipInputStream.getNextEntry();
                LOGGER.info("reading zip entry: " + zipEntry.getName());
            } catch (IOException ex) {
                final String message = "could not execute '" + this.getTaskName()
                            + "': could process zip file from body parameter: "
                            + ex.getMessage();
                LOGGER.error(message);
                throw new RuntimeException(message);
            }
        }

        // TODO: use thread to parallelise multiple exports
        int i = 1;
        for (final ExportTheme exportTheme : exportOptions.getExportThemes()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("executing parallel export '" + exportTheme.getTitle() + "' #"
                            + i + "/" + exportOptions.getExportThemes().size());
            }

            final AbstractExportAction exportAction = this.exportActions.get(exportTheme.getClassName());
            if (exportAction == null) {
                final String message = "could not execute '" + this.getTaskName()
                            + "': legacy ExportAction not found: unsupported export theme '"
                            + exportTheme.getClassName() + "'!";
                LOGGER.error(message);
                throw new RuntimeException(message);
            }

            // this is the export format of the legacy export action (SHP in case of merge datasource = true)
            final String exportFormat = exportOptions.isMergeExternalDatasource()
                ? AbstractExportAction.PARAM_EXPORTFORMAT_SHP : EXPORT_FORMATS.get(exportTheme.getExportFormat());
            final ServerActionParameter[] serverActionParameters = this.createServerActionParameters(
                    exportTheme,
                    exportFormat);

            if ((serverActionParameters == null) || (serverActionParameters.length == 0)) {
                final String message = "could not execute '" + this.getTaskName()
                            + "': could not generate Server Action Parameters for export theme '"
                            + exportTheme.getClassName() + "'!";
                LOGGER.error(message);
                throw new RuntimeException(message);
            }

            final Future<Object> exportResultFuture = executorService.submit(new Callable<Object>() {

                        @Override
                        public Object call() {
                            // execute the legacy export .........
                            final Object exportResult = exportAction.execute(null, serverActionParameters);
                            return exportResult;
                        }
                    });

            exportResults.put(exportTheme, exportResultFuture);
            i++;
        }

        if (exportResults.isEmpty()) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': legacy export actions did not produce any result!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        } else if (exportResults.size() != exportOptions.getExportThemes().size()) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': only " + exportResults.size() + " of " + exportOptions.getExportThemes().size()
                        + " legacy export actions didi produce a result!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        /*if(exportOptions.isMergeExternalDatasource()) {
         *      //TODO: implement merging with external datasource } else {*/
        // one already zipped SHP File exported
        if ((exportResults.size() == 1)) {
            try {
                final Object exportResult = exportResults.values().iterator().next().get();
                final String contentType;

                if ("csv".equalsIgnoreCase(exportOptions.getExportFormat())) {
                    if (!(exportResult instanceof String)) {
                        final String message = "could not execute '" + this.getTaskName()
                                    + "': expected result (" + exportOptions.getExportFormat() + ") of '"
                                    + exportResults.keySet().iterator().next().getClassName()
                                    + "' is not of type String: " + exportResult.getClass().getSimpleName();
                        LOGGER.error(message);
                        throw new RuntimeException(message);
                    }
                } else if (!(exportResult instanceof byte[])) {
                    final String message = "could not execute '" + this.getTaskName()
                                + "': expected result (" + exportOptions.getExportFormat() + ") of '"
                                + exportResults.keySet().iterator().next().getClassName()
                                + "' is not of type byte[]: " + exportResult.getClass().getSimpleName();
                    LOGGER.error(message);
                    throw new RuntimeException(message);
                }

                contentType = CONTENT_TYPES.get(EXPORT_FORMATS.get(exportOptions.getExportFormat()));
                if (contentType == null) {
                    final String message = "could not execute '" + this.getTaskName()
                                + "': contant type of result (" + exportOptions.getExportFormat() + ") not found!";
                    LOGGER.error(message);
                    throw new RuntimeException(message);
                }

                LOGGER.info("Export successfully performed for " + exportOptions.getExportThemes()
                            + " themes to '" + MediaTypes.APPLICATION_ZIP
                            + "' in " + ((System.currentTimeMillis() - current) / 1000) + "s.");
                return new GenericResourceWithContentType(contentType, exportResult);
            } catch (final InterruptedException | ExecutionException ex) {
                final String message = "could not execute '" + this.getTaskName()
                            + "': error during thread execution: " + ex.getMessage();
                LOGGER.error(message, ex);
                throw new RuntimeException(message, ex);
            }
        } else {
            final ByteArrayOutputStream combinedExportResults = new ByteArrayOutputStream();
            try(final ZipOutputStream zipStream = new ZipOutputStream(combinedExportResults)) {
                for (final ExportTheme exportTheme : exportResults.keySet()) {
                    final Object exportResult = exportResults.get(exportTheme).get();
                    final String extension =
                        AbstractExportAction.PARAM_EXPORTFORMAT_SHP.equals(EXPORT_FORMATS.get(
                                exportTheme.getExportFormat())) ? "zip" : exportTheme.getExportFormat();

                    zipStream.putNextEntry(new ZipEntry(exportTheme.getTitle() + "." + extension));

                    if (exportResult instanceof byte[]) {
                        zipStream.write((byte[])exportResult);
                    } else if (exportResult instanceof String) {
                        zipStream.write(((String)exportResult).getBytes("UTF-8"));
                    } else {
                        final String message = "could not execute '" + this.getTaskName()
                                    + "': expected result of '" + exportTheme.getClassName()
                                    + "' is not of a suopprted type (byte[] or String) but '"
                                    + exportResult.getClass().getSimpleName() + "'!";
                        LOGGER.error(message);
                        throw new RuntimeException(message);
                    }

                    zipStream.closeEntry();
                }
            } catch (final RuntimeException re) {
                throw re;
            } catch (final IOException ioex) {
                final String message = "could not execute '" + this.getTaskName()
                            + "': error during zipping combined results: " + ioex.getMessage();
                LOGGER.error(message, ioex);
                throw new RuntimeException(message, ioex);
            } catch (final InterruptedException | ExecutionException ex) {
                final String message = "could not execute '" + this.getTaskName()
                            + "': error during thread execution: " + ex.getMessage();
                LOGGER.error(message, ex);
                throw new RuntimeException(message, ex);
            }

            final byte[] result = combinedExportResults.toByteArray();

            LOGGER.info("Export successfully performed for " + exportOptions.getExportThemes().size()
                        + " themes in " + ((System.currentTimeMillis() - current) / 1000) + "s.");

            return new GenericResourceWithContentType(MediaTypes.APPLICATION_ZIP, result);
        }
    }

    /**
     * Creates ServerActionParameters for legacy eyport actions. Note: if merge external datasource is true, the export
     * format is always the to SHP to allow for spatial merging of the exported dataset with the external datasource
     * (SHP File) that was provided with the body parameter of the this server action.
     *
     * @param   exportTheme   DOCUMENT ME!
     * @param   exportFormat  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  RuntimeException  DOCUMENT ME!
     */
    private ServerActionParameter[] createServerActionParameters(
            final ExportTheme exportTheme,
            final String exportFormat) throws RuntimeException {
        final ArrayList<ServerActionParameter> sapList = new ArrayList<ServerActionParameter>();

        ServerActionParameter serverActionParameter;

        // name
        if (exportTheme.getTitle().isEmpty()) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': empty 'title' property for export theme '" + exportTheme.getClassName() + "'!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        serverActionParameter = new ServerActionParameter<String>(PARAM_NAME, exportTheme.getTitle());
        sapList.add(serverActionParameter);

        // export format
        serverActionParameter = new ServerActionParameter<String>(PARAM_EXPORTFORMAT, exportFormat);
        sapList.add(serverActionParameter);

        // internal export
        serverActionParameter = new ServerActionParameter<Boolean>(PARAM_INTERNAL, true);
        sapList.add(serverActionParameter);

        // parameters
        if (exportTheme.getParameters().isEmpty()) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': empty export parameters list for export theme '" + exportTheme.getClassName() + "'!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }
        serverActionParameter = new ServerActionParameter<Collection<Parameter>>(
                PARAM_PARAMETER,
                exportTheme.getParameters());
        sapList.add(serverActionParameter);

        // object PKs
        if (exportTheme.getExportPKs().isEmpty()) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': empty export PKs list for export theme '" + exportTheme.getClassName() + "'!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        // action specific PKs in DWH
        if ("BORIS_SITE".equalsIgnoreCase(exportTheme.getClassName())) {
            serverActionParameter = new ServerActionParameter<Collection<String>>(
                    BorisExportAction.PARAM_STANDORTE,
                    exportTheme.getExportPKs());
        } else if ("WAGW_STATION".equalsIgnoreCase(exportTheme.getClassName())
                    || "WAOW_STATION".equalsIgnoreCase(exportTheme.getClassName())) {
            serverActionParameter = new ServerActionParameter<Collection<String>>(
                    WaExportAction.PARAM_MESSSTELLEN,
                    exportTheme.getExportPKs());
        } else if ("EPRTR_INSTALLATION".equalsIgnoreCase(exportTheme.getClassName())) {
            final Collection<Long> installationIds = new ArrayList<Long>(exportTheme.getExportPKs().size());
            for (final String exportPk : exportTheme.getExportPKs()) {
                installationIds.add(Long.parseLong(exportPk));
            }

            serverActionParameter = new ServerActionParameter<Collection<Long>>(
                    EprtrExportAction.PARAM_INSTALLATIONS,
                    installationIds);
        } else if ("MOSS".equalsIgnoreCase(exportTheme.getClassName())) {
            if ((exportTheme.getObjectIds() == null) || exportTheme.getObjectIds().isEmpty()) {
                final String message = "could not execute '" + this.getTaskName()
                            + "': empty export OBJECT IDs list for export theme '" + exportTheme.getClassName() + "'!";
                LOGGER.error(message);
                throw new RuntimeException(message);
            }

            // YES, object Ids, not SampleIDs! (SampleIds only needed for export from konvert_join_95_10_final.xls!
            serverActionParameter = new ServerActionParameter<Collection<Long>>(
                    MossExportAction.PARAM_OBJECT_IDS,
                    exportTheme.getObjectIds());
        } else {
            final String message = "could not execute '" + this.getTaskName()
                        + "': could not generate server action parameters, unsupported export Theme '"
                        + exportTheme.getClassName() + "'!";
            LOGGER.error(message);
            throw new RuntimeException(message);
        }

        sapList.add(serverActionParameter);

        return (ServerActionParameter[])sapList.toArray(new ServerActionParameter[sapList.size()]);
    }

    @Override
    public String getTaskName() {
        return TASK_NAME;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  DOCUMENT ME!
     */
    public static void main(final String[] args) {
        try {
            final Properties log4jProperties = new Properties();
            log4jProperties.put("log4j.appender.Remote", "org.apache.log4j.net.SocketAppender");
            log4jProperties.put("log4j.appender.Remote.remoteHost", "localhost");
            log4jProperties.put("log4j.appender.Remote.port", "4445");
            log4jProperties.put("log4j.appender.Remote.locationInfo", "true");

            log4jProperties.put(
                "log4j.logger.de.cismet",
                "ALL,Remote");

            PropertyConfigurator.configure(log4jProperties);
            
            
            final String exportOptionsJson = IOUtils.toString(RestApiExportAction.class.getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/rest/exportOptions.json"),
                    "UTF-8");
            

            /*final String GCS_WGS_1984 = IOUtils.toString(RestApiExportAction.class.getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/GCS_WGS_1984.prj"),
                    "UTF-8");
            final String MGI_Austria_Lambert = IOUtils.toString(RestApiExportAction.class.getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/MGI_Austria_Lambert.prj"),
                    "UTF-8");
            
            final CoordinateReferenceSystem GCS_WGS_1984_crs = CRS.parseWKT(GCS_WGS_1984); 
            final CoordinateReferenceSystem MGI_Austria_Lambert_crs = CRS.parseWKT(MGI_Austria_Lambert); 
            
            //System.out.println("GCS_WGS_1984 CRS = " + CRS.lookupEpsgCode(GCS_WGS_1984_crs, true));
            System.out.println("GCS_WGS_1984 CRS = " + CRS.lookupIdentifier(GCS_WGS_1984_crs, true));
            //System.out.println("MGI_Austria_Lambert CRS = " + CRS.lookupEpsgCode(MGI_Austria_Lambert_crs, true));
            System.out.println("MGI_Austria_Lambert CRS = " + CRS.lookupIdentifier(MGI_Austria_Lambert_crs, true));*/
            
            System.out.println("Bessel_1841_Lambert_Conformal_Conic = " + CRS.lookupIdentifier(CRS.parseWKT("PROJCS[\"Bessel_1841_Lambert_Conformal_Conic\",GEOGCS[\"GCS_Bessel_1841\",DATUM[\"D_Bessel_1841\",SPHEROID[\"Bessel_1841\",6377397.155,299.1528128]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Conformal_Conic\"],PARAMETER[\"False_Easting\",400000.0],PARAMETER[\"False_Northing\",400000.0],PARAMETER[\"Central_Meridian\",13.33333333333333],PARAMETER[\"Standard_Parallel_1\",46.0],PARAMETER[\"Standard_Parallel_2\",49.0],PARAMETER[\"Latitude_Of_Origin\",47.5],UNIT[\"Meter\",1.0]]"), false));
            System.out.println("LAM_CC_4730_AUT = " + CRS.lookupIdentifier(CRS.parseWKT("PROJCS[\"LAM_CC_4730_AUT\",GEOGCS[\"GCS_MGI\",DATUM[\"D_MGI\",SPHEROID[\"Bessel_1841\",6377397.155,299.1528128]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Conformal_Conic\"],PARAMETER[\"False_Easting\",400000.0],PARAMETER[\"False_Northing\",400000.0],PARAMETER[\"Central_Meridian\",13.33333333333333],PARAMETER[\"Standard_Parallel_1\",46.0],PARAMETER[\"Standard_Parallel_2\",49.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",47.5],UNIT[\"Meter\",1.0]]"), false));
            System.out.println("LAMBERT = " + CRS.lookupIdentifier(CRS.parseWKT("PROJCS[\"LAMBERT\",GEOGCS[\"GCS_MGI\",DATUM[\"D_MGI\",SPHEROID[\"Bessel_1841\",6377397.155,299.1528128]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Lambert_Conformal_Conic\"],PARAMETER[\"False_Easting\",400000.0],PARAMETER[\"False_Northing\",400000.0],PARAMETER[\"Central_Meridian\",13.33333333333333],PARAMETER[\"Standard_Parallel_1\",46.0],PARAMETER[\"Standard_Parallel_2\",49.0],PARAMETER[\"Scale_Factor\",1.0],PARAMETER[\"Latitude_Of_Origin\",47.5],UNIT[\"Meter\",1.0]]"), false));
            System.out.println("Austria Lambert = " + CRS.lookupIdentifier(CRS.parseWKT("PROJCS[\"MGI / Austria Lambert\", GEOGCS[\"MGI\", DATUM[\"Militar-Geographische Institut\", SPHEROID[\"Bessel 1841\", 6377397.155, 299.1528128, AUTHORITY[\"EPSG\",\"7004\"]], TOWGS84[601.705, 84.263, 485.227, 4.7354, -1.3145, -5.393, -2.3887], AUTHORITY[\"EPSG\",\"6312\"]], PRIMEM[\"Greenwich\", 0.0, AUTHORITY[\"EPSG\",\"8901\"]], UNIT[\"degree\", 0.017453292519943295], AXIS[\"Geodetic longitude\", EAST], AXIS[\"Geodetic latitude\", NORTH], AUTHORITY[\"EPSG\",\"4312\"]], PROJECTION[\"Lambert_Conformal_Conic_2SP\"], PARAMETER[\"central_meridian\", 13.333333333333334], PARAMETER[\"latitude_of_origin\", 47.5], PARAMETER[\"standard_parallel_1\", 49.0], PARAMETER[\"false_easting\", 400000.0], PARAMETER[\"false_northing\", 400000.0], PARAMETER[\"scale_factor\", 1.0], PARAMETER[\"standard_parallel_2\", 46.0], UNIT[\"m\", 1.0], AXIS[\"Easting\", EAST], AXIS[\"Northing\", NORTH], AUTHORITY[\"EPSG\",\"31287\"]]"), false));
            System.out.println("GCS_WGS_1984 = " + CRS.lookupIdentifier(CRS.parseWKT("GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\",0.017453292519943295]]"), true));
            System.out.println("MGI_Austria_Lambert = " + CRS.lookupIdentifier(CRS.parseWKT("PROJCS[\"MGI_Austria_Lambert\",GEOGCS[\"GCS_MGI\",DATUM[\"D_MGI\",SPHEROID[\"Bessel_1841\",6377397.155,299.1528128]],PRIMEM[\"Greenwich\",0],UNIT[\"Degree\",0.017453292519943295]],PROJECTION[\"Lambert_Conformal_Conic\"],PARAMETER[\"standard_parallel_1\",49],PARAMETER[\"standard_parallel_2\",46],PARAMETER[\"latitude_of_origin\",47.5],PARAMETER[\"central_meridian\",13.33333333333333],PARAMETER[\"false_easting\",400000],PARAMETER[\"false_northing\",400000],UNIT[\"Meter\",1]]"), false));
            System.out.println("MGI = " + CRS.lookupIdentifier(CRS.parseWKT("PROJCS[\"MGI / Austria Lambert\",\n" +
                "  BASEGEODCRS[\"MGI\",\n" +
                "    DATUM[\"Militar-Geographische Institut\",\n" +
                "      ELLIPSOID[\"Bessel 1841\",6377397.155,299.1528128,LENGTHUNIT[\"metre\",1.0]]]],\n" +
                "  CONVERSION[\"Austria Lambert\",\n" +
                "    METHOD[\"Lambert Conic Conformal (2SP)\",ID[\"EPSG\",9802]],\n" +
                "    PARAMETER[\"Latitude of false origin\",47.5,ANGLEUNIT[\"degree\",0.01745329252]],\n" +
                "    PARAMETER[\"Longitude of false origin\",13.333333333333,ANGLEUNIT[\"degree\",0.01745329252]],\n" +
                "    PARAMETER[\"Latitude of 1st standard parallel\",49,ANGLEUNIT[\"degree\",0.01745329252]],\n" +
                "    PARAMETER[\"Latitude of 2nd standard parallel\",46,ANGLEUNIT[\"degree\",0.01745329252]],\n" +
                "    PARAMETER[\"Easting at false origin\",400000,LENGTHUNIT[\"metre\",1.0]],\n" +
                "    PARAMETER[\"Northing at false origin\",400000,LENGTHUNIT[\"metre\",1.0]]],\n" +
                "  CS[cartesian,2],\n" +
                "    AXIS[\"northing (X)\",north,ORDER[1]],\n" +
                "    AXIS[\"easting (Y)\",east,ORDER[2]],\n" +
                "    LENGTHUNIT[\"metre\",1.0],\n" +
                "  ID[\"EPSG\",31287]]"), false));

            System.exit(0);

            final ServerActionParameter[] serverActionParameters = new ServerActionParameter[] {
                    new ServerActionParameter<String>(PARAM_EXPORT_OPTIONS, exportOptionsJson)
                };

            final RestApiExportAction exportAction = new RestApiExportAction();

            final GenericResourceWithContentType result = exportAction.execute("DUMMY OBJECT", serverActionParameters);
            final Path file = Files.write(Paths.get("restApiExport.zip"), (byte[])result.getRes());

            System.out.println("Export File written to "
                        + file.toAbsolutePath().toString());
            System.exit(0);
        } catch (Throwable ex) {
            RestApiExportAction.LOGGER.fatal(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
