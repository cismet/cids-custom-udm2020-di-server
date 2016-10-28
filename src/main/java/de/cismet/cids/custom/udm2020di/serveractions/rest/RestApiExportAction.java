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

import java.sql.ResultSet;

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
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_EXPORTFORMAT_CSV;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_EXPORTFORMAT_SHP;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_EXPORTFORMAT_XLSX;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_INTERNAL;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_NAME;
import static de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction.PARAM_PARAMETER;

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

    public static final int GEOJSON_CRS = 4326;

    public static final String PARAM_EXPORT_OPTIONS = "exportOptions";
    // public static final String PARAM_MERGE_DATASOURCE = "isMergeExternalDatasource";

    public static final HashMap<String, String> EXPORT_FORMATS = new HashMap<String, String>();
    public static final HashMap<String, String> CONTENT_TYPES = new HashMap<String, String>();
    public static final HashMap<String, Integer> EXPORT_CRS = new HashMap<String, Integer>();

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

        EXPORT_CRS.put("BORIS_SITE", 31287);
        EXPORT_CRS.put("WAGW_STATION", 31287);
        EXPORT_CRS.put("WAOW_STATION", 31287);
        EXPORT_CRS.put("EPRTR_INSTALLATION", 4326);
        EXPORT_CRS.put("MOSS", 4326);
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
        actionInfo.setParameterDescription(parameterDescriptions);

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
                    this.exportActions.put("BORIS_SITE", new BorisExportAction());
                    this.exportActions.put("WAGW_STATION", new WagwExportAction());
                    this.exportActions.put("WAOW_STATION", new WaowExportAction());
                    this.exportActions.put("EPRTR_INSTALLATION", new EprtrExportAction());
                    this.exportActions.put("MOSS", new MossExportAction());
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
        final GenericResourceWithContentType actionResult;

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
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(exportOptionsJson);
        }
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

        int i = 1;
        for (final ExportTheme exportTheme : exportOptions.getExportThemes()) {
            if (exportOptions.isMergeExternalDatasource() == true) {
                if (exportTheme.getExportDatasource() == null) {
                    final String message = "could not execute '" + this.getTaskName()
                                + "': export data source of '" + exportTheme.getTitle() + "' is null!";
                    LOGGER.error(message);
                }

                if (exportTheme.getExportDatasource().getParameters().isEmpty()) {
                    final String message = "could not execute '" + this.getTaskName()
                                + "': parameter list of export data source ("
                                + exportTheme.getExportDatasource().getFilename()
                                + ") of '" + exportTheme.getTitle() + "' is null!";
                    LOGGER.error(message);
                }
            }

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

        if (exportOptions.isMergeExternalDatasource()) {
            final LinkedHashMap<ExportTheme, Future<Object>> mergedExportResults =
                new LinkedHashMap<ExportTheme, Future<Object>>();
            int j = 1;
            for (final ExportTheme exportTheme : exportResults.keySet()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("executing parallel merging of '" + exportTheme.getTitle()
                                + " ' with '" + exportTheme.getExportDatasource().getName()
                                + "' #" + j + "/" + exportOptions.getExportThemes().size());
                }

                try {
                    final Object exportResult = exportResults.get(exportTheme).get();
                    if (!(exportResult instanceof byte[])) {
                        final String message = "could not execute '" + getTaskName()
                                    + "': result of SHP Export of '" + exportTheme.getTitle()
                                    + "' is not a byte array (zipped SHP file)";
                        LOGGER.error(message);
                        throw new RuntimeException(message);
                    }

                    final Future<Object> mergedExportResultFuture = executorService.submit(new Callable<Object>() {

                                @Override
                                public Object call() {
                                    final Object mergedExportResult = mergeWithExternalDatasource(
                                            exportTheme,
                                            (byte[])exportResult,
                                            (byte[])body);
                                    return mergedExportResult;
                                }
                            });
                    mergedExportResults.put(exportTheme, mergedExportResultFuture);
                } catch (final InterruptedException | ExecutionException ex) {
                    final String message = "could not execute '" + this.getTaskName()
                                + "': error during thread execution: " + ex.getMessage();
                    LOGGER.error(message, ex);
                    throw new RuntimeException(message, ex);
                }
                j++;
            }
            actionResult = this.processExportResults(exportOptions, mergedExportResults);
        } else {
            actionResult = this.processExportResults(exportOptions, exportResults);
        }

        LOGGER.info("Export successfully performed for " + exportOptions.getExportThemes().size()
                    + " themes to '" + MediaTypes.APPLICATION_ZIP
                    + "' in " + ((System.currentTimeMillis() - current) / 1000) + "s.");
        return actionResult;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   exportTheme         DOCUMENT ME!
     * @param   exportResult        DOCUMENT ME!
     * @param   externalDatasource  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  RuntimeException  DOCUMENT ME!
     */
    private Object mergeWithExternalDatasource(
            final ExportTheme exportTheme,
            final byte[] exportResult,
            final byte[] externalDatasource) throws RuntimeException {
        H2GeoJsonJoiner geoJsonJoiner = null;
        try {
            final int exportSrs = EXPORT_CRS.get(exportTheme.getClassName());
            final AbstractExportAction exportAction = this.exportActions.get(exportTheme.getClassName());
            final String exportFormat = EXPORT_FORMATS.get(exportTheme.getExportFormat());
            geoJsonJoiner = new H2GeoJsonJoiner(
                    exportResult,
                    externalDatasource,
                    exportTheme.getExportDatasource().getParameters(),
                    exportSrs,
                    GEOJSON_CRS);

            final ResultSet mergedResultSet = geoJsonJoiner.getResultSet();
            final Object result;
            if (exportTheme.getExportFormat().equalsIgnoreCase(PARAM_EXPORTFORMAT_CSV)) {
                result = exportAction.createCsv(mergedResultSet, exportTheme.getTitle(), false);
            } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_XLSX)) {
                result = exportAction.createXlsx(mergedResultSet, exportTheme.getTitle());
            } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_SHP)) {
                result = exportAction.createShapeFile(mergedResultSet, exportTheme.getTitle());
            } else {
                final String message = "could not execute '" + this.getTaskName()
                            + "': error during thread execution: unsupported export format '"
                            + exportFormat + "'";
                LOGGER.error(message);
                throw new RuntimeException(message);
            }

            return result;
        } catch (final RuntimeException re) {
            throw re;
        } catch (final Exception e) {
            final String message = "could not execute '" + this.getTaskName()
                        + "': error during merging external datasource with '"
                        + exportTheme.getTitle() + "': " + e.getMessage();
            LOGGER.error(message, e);
            throw new RuntimeException(message, e);
        } finally {
            if (geoJsonJoiner != null) {
                geoJsonJoiner.close();
            }
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   exportOptions  DOCUMENT ME!
     * @param   exportResults  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  RuntimeException  DOCUMENT ME!
     */
    private GenericResourceWithContentType processExportResults(final ExportOptions exportOptions,
            final LinkedHashMap<ExportTheme, Future<Object>> exportResults) throws RuntimeException {
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
                                + "': content type of result (" + exportOptions.getExportFormat() + ") not found!";
                    LOGGER.error(message);
                    throw new RuntimeException(message);
                }

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
            final byte[] geoJson = IOUtils.toByteArray(RestApiExportAction.class.getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/rest/FAO_Bodentypen.zip"));

            final ServerActionParameter[] serverActionParameters = new ServerActionParameter[] {
                    new ServerActionParameter<String>(PARAM_EXPORT_OPTIONS, exportOptionsJson)
                };

            final RestApiExportAction exportAction = new RestApiExportAction();

            final GenericResourceWithContentType result = exportAction.execute(geoJson, serverActionParameters);
            final Path file = Files.write(Paths.get("restApiExport.xlsx"), (byte[])result.getRes());

            System.out.println("Export File (" + result.getContentType() + ") written to "
                        + file.toAbsolutePath().toString());
            System.exit(0);
        } catch (Throwable ex) {
            RestApiExportAction.LOGGER.fatal(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
