/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.boris;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

import org.deegree.datatypes.Types;
import org.deegree.datatypes.UnknownTypeException;
import org.deegree.io.dbaseapi.DBaseException;
import org.deegree.model.feature.FeatureCollection;
import org.deegree.model.spatialschema.GeometryException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.text.NumberFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.indeximport.boris.BorisImport;
import de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction;
import de.cismet.cids.custom.udm2020di.types.Parameter;

import de.cismet.cids.server.actions.ServerAction;
import de.cismet.cids.server.actions.ServerActionParameter;

import de.cismet.cismap.commons.features.DefaultFeatureServiceFeature;
import de.cismet.cismap.commons.features.FeatureServiceFeature;
import de.cismet.cismap.commons.featureservice.DefaultLayerProperties;
import de.cismet.cismap.commons.featureservice.FeatureServiceAttribute;
import de.cismet.cismap.commons.gui.shapeexport.ShapeExportHelper;
import de.cismet.cismap.commons.tools.SimpleFeatureCollection;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@org.openide.util.lookup.ServiceProvider(service = ServerAction.class)
public class BorisExportAction extends AbstractExportAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String TASK_NAME = "borisExportAction";
    public static final String PARAM_STANDORTE = "standorte";
    public static final int BORIS_EPSG = 31287;

    //~ Instance fields --------------------------------------------------------

    protected String decodeSampleValuesStatementTpl;
    protected String exportBorisMesswerteStatementTpl;
    /**
     * Standard BORIS Export does not contain coordinates (data protection restriction), therfore also common ShapeFile
     * Support is disabled. However for merging BORIS Exports with External Data (Shapefiles trasferred as GeoJson), a
     * ShapeFile is required anyway. The export Boris Messwerte To Shapefile Statement is therfore used in an INTERNAL
     * export initiated by the RestApiExportAction!
     */
    protected String exportBorisMesswerteToShapefileStatementTpl;
    protected String projectionFile;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BorisExportAction object.
     */
    public BorisExportAction() {
        super();
        this.log = Logger.getLogger(BorisExportAction.class);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a new CsvExportAction object.
     *
     * @return  DOCUMENT ME!
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    private boolean init() throws IOException, ClassNotFoundException, SQLException {
        if (this.isInitialised()) {
            log.error(this.getClass().getSimpleName() + " is already initialised!");
            return this.isInitialised();
        }

        final boolean isInitialised = super.init(BorisImport.class.getResourceAsStream("boris.properties"));
        if (isInitialised) {
            this.decodeSampleValuesStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/boris/decode-boris-messwerte.tpl.sql"),
                    "UTF-8");

            this.exportBorisMesswerteStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/boris/export-boris-messwerte.tpl.sql"),
                    "UTF-8");

            this.exportBorisMesswerteToShapefileStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/boris/export-boris-messwerte-to-shapefile.tpl.sql"),
                    "UTF-8");

            this.projectionFile = IOUtils.toString(this.getClass().getResourceAsStream(
                        "/de/cismet/cids/custom/udm2020di/dataexport/MGI_Austria_Lambert.prj"),
                    "UTF-8");

            log.info(this.getClass().getSimpleName() + " initialised");
        }

        return isInitialised;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   exportTpl    DOCUMENT ME!
     * @param   standortPks  DOCUMENT ME!
     * @param   parameters   DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createExportBorisMesswerteStatement(
            final String exportTpl,
            final Collection<String> standortPks,
            final Collection<Parameter> parameters) {
        if (log.isDebugEnabled()) {
            log.debug("creating export statements for " + standortPks.size() + "standorte and "
                        + parameters.size() + parameters);
        }

        final StringBuilder standorteBuilder = new StringBuilder();
        final Iterator<String> standortPksIterator = standortPks.iterator();
        while (standortPksIterator.hasNext()) {
            standorteBuilder.append('\'').append(standortPksIterator.next()).append('\'');
            if (standortPksIterator.hasNext()) {
                standorteBuilder.append(',');
            }
        }

        final StringBuilder decodeBuilder = new StringBuilder();
        final StringBuilder parameterBuilder = new StringBuilder();
        final Iterator<Parameter> parametersIterator = parameters.iterator();
        while (parametersIterator.hasNext()) {
            final Parameter parameter = parametersIterator.next();

            // decode stuff
            String tmpDecodeString = this.decodeSampleValuesStatementTpl.replace(
                    "%PARAMETER_PK%",
                    parameter.getParameterPk());
            final String parameterName = (parameter.getParameterName().length() > 28)
                ? parameter.getParameterName().substring(0, 28) : parameter.getParameterName();
            tmpDecodeString = tmpDecodeString.replace("%PARAMETER_NAME%", parameterName);
            decodeBuilder.append(tmpDecodeString);
            if (parametersIterator.hasNext()) {
                decodeBuilder.append(", \n");
            }

            parameterBuilder.append('\'').append(parameter.getParameterPk()).append('\'');
            if (parametersIterator.hasNext()) {
                parameterBuilder.append(',');
            }
        }

        String exportBorisMesswerteStatement = exportTpl.replace(
                "%MESSWERT_DECODE_STATEMENTS%",
                decodeBuilder);
        exportBorisMesswerteStatement = exportBorisMesswerteStatement.replace(
                "%MESSWERT_PARAMETER_PKS%",
                parameterBuilder);
        exportBorisMesswerteStatement = exportBorisMesswerteStatement.replace("%STANDORT_PKS%", standorteBuilder);
        if (log.isDebugEnabled()) {
            log.debug(exportBorisMesswerteStatement);
        }
        return exportBorisMesswerteStatement;
    }

    @Override
    public Object execute(final Object body, final ServerActionParameter... params) {
        Statement exportBorisMesswerteStatement = null;
        ResultSet exportBorisMesswerteResult = null;
        try {
            if (!this.isInitialised()) {
                log.info("performing lazy initilaisation of " + this.getClass().getSimpleName());
                this.initialised = this.init();
            }

            this.checkConnection();

            Object result = null;

            Collection<String> standortPks = null;
            Collection<Parameter> parameters = null;
            String exportFormat = PARAM_EXPORTFORMAT_CSV;
            String name = "export";
            boolean isInternal = false;

            for (final ServerActionParameter param : params) {
                if (param.getKey().equalsIgnoreCase(PARAM_STANDORTE)) {
                    standortPks = (Collection<String>)param.getValue();
                } else if (param.getKey().equalsIgnoreCase(PARAM_PARAMETER)) {
                    parameters = (Collection<Parameter>)param.getValue();
                } else if (param.getKey().equalsIgnoreCase(PARAM_EXPORTFORMAT)) {
                    exportFormat = param.getValue().toString();
                } else if (param.getKey().equalsIgnoreCase(PARAM_NAME)) {
                    name = param.getValue().toString();
                } else if (param.getKey().equalsIgnoreCase(PARAM_INTERNAL)) {
                    isInternal = (boolean)param.getValue();
                } else {
                    log.warn("ignoring unsupported server action parameter: '"
                                + param.getKey() + "' = '" + param.getValue() + "'!");
                }
            }

            if ((standortPks != null) && (parameters != null)) {
                log.info("performing " + ((isInternal == true) ? "INTERNAL '" : "'") + TASK_NAME + "' for "
                            + standortPks.size()
                            + " BORIS STANDORTE and " + parameters.size() + " parameters to '"
                            + name + "' (" + exportFormat + ")");

                final String exportTpl = exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_SHP)
                    ? this.exportBorisMesswerteToShapefileStatementTpl : this.exportBorisMesswerteStatementTpl;
                final String exportBorisMesswerte = this.createExportBorisMesswerteStatement(
                        exportTpl,
                        standortPks,
                        parameters);

                exportBorisMesswerteStatement = this.sourceConnection.createStatement();
                exportBorisMesswerteResult = exportBorisMesswerteStatement.executeQuery(exportBorisMesswerte);

                if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_CSV)) {
                    result = this.createCsv(exportBorisMesswerteResult, name, false);
                } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_XLSX)) {
                    result = this.createXlsx(exportBorisMesswerteResult, name);
                } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_SHP)) {
                    if (isInternal == true) {
                        log.warn("performing INTERNAL SHP Export of BORIS Stations!");
                        result = this.createShapeFile(exportBorisMesswerteResult, name);
                    } else {
                        final String message = "SHP Export of BORIS Stations not permitted!";
                        log.error(message);
                        throw new Exception(message);
                    }
                } else {
                    final String message = "unsupported export format '" + exportFormat + "'";
                    log.error(message);
                    throw new Exception(message);
                }

                exportBorisMesswerteStatement.close();
            } else {
                log.error("no PARAM_STANDORTE and PARAM_PARAMETER server action parameters provided,"
                            + "returning null");
            }

            return result;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            if (exportBorisMesswerteResult != null) {
                try {
                    exportBorisMesswerteResult.close();
                } catch (Exception e) {
                    log.error("could not close exportBorisMesswerteResult", e);
                }
            }

            if (exportBorisMesswerteStatement != null) {
                try {
                    exportBorisMesswerteStatement.close();
                } catch (Exception e) {
                    log.error("could not close exportBorisMesswerteStatement", e);
                }
            }

            throw new RuntimeException(ex);
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   resultSet  DOCUMENT ME!
     * @param   name       DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException          DOCUMENT ME!
     * @throws  DBaseException        DOCUMENT ME!
     * @throws  GeometryException     DOCUMENT ME!
     * @throws  IOException           DOCUMENT ME!
     * @throws  UnknownTypeException  org.deegree.datatypes.UnknownTypeException
     * @throws  Exception             DOCUMENT ME!
     */
    @Override
    public byte[] createShapeFile(final ResultSet resultSet, final String name) throws SQLException,
        DBaseException,
        GeometryException,
        IOException,
        UnknownTypeException,
        Exception {
        final PrecisionModel precisionModel = new PrecisionModel(PrecisionModel.FLOATING);
        final GeometryFactory geometryFactory = new GeometryFactory(precisionModel, BORIS_EPSG);
        final ResultSetMetaData metaData = resultSet.getMetaData();

        final int columnCount = metaData.getColumnCount();
        final List<String[]> aliasAttributeList = new ArrayList<String[]>();
        final Map<String, FeatureServiceAttribute> propertyTypesMap =
            new LinkedHashMap<String, FeatureServiceAttribute>();

        this.fillPropertyTypesMap(metaData, columnCount, aliasAttributeList, propertyTypesMap);

        final List<FeatureServiceFeature> featureList = new ArrayList<FeatureServiceFeature>();
        int rowNum = 0;

        while (resultSet.next()) {
            rowNum++;

            // final String STANDORT_PK = resultSet.getString("STANDORT_PK");
            final float RECHTSWERT = resultSet.getFloat("RECHTSWERT");
            final float HOCHWERT = resultSet.getFloat("HOCHWERT");
            final String PROBE_PK = resultSet.getString("PROBE_PK");

            int id;
            try {
                id = Integer.parseInt(PROBE_PK);
            } catch (Exception ex) {
                id = rowNum;
            }

            final Geometry geometry = geometryFactory.createPoint(new Coordinate(RECHTSWERT, HOCHWERT));
            final FeatureServiceFeature feature = this.createFeatureServiceFeature(
                    id,
                    geometry,
                    resultSet,
                    metaData,
                    columnCount);
            featureList.add(feature);
        }

        final FeatureCollection featureCollection = new SimpleFeatureCollection(
                String.valueOf(System.currentTimeMillis()),
                featureList.toArray(new FeatureServiceFeature[featureList.size()]),
                aliasAttributeList,
                propertyTypesMap);

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final ZipOutputStream zipStream = new ZipOutputStream(output);
        ShapeExportHelper.writeShapeFileToZip(
            featureCollection,
            name,
            new File(System.getProperty("java.io.tmpdir")),
            zipStream,
            this.projectionFile);

        zipStream.flush();
        zipStream.finish();

        final byte[] result = output.toByteArray();

        zipStream.close();
        output.close();

        return result;

//        final ShapeFileWriter writer = new ShapeFileWriter(shapeFile);
//        writer.write();
//        return shapeFile;
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
            final Collection<String> standortPks = Arrays.asList(
                    new String[] { "1000066", "1000067", "1000068", "1000069", "1000070", "1000071", "1000072" });

            final Collection<Parameter> parameter = Arrays.asList(
                    new Parameter[] {
                        new Parameter("BCU1", "Kü ü ü pfer"),
                        new Parameter("BZN1", "Zinn"),
                        new Parameter("BPB1", "Blei"),
                    });

            final ServerActionParameter[] serverActionParameters = new ServerActionParameter[] {
                    new ServerActionParameter<Collection<String>>(PARAM_STANDORTE, standortPks),
                    new ServerActionParameter<Collection<Parameter>>(PARAM_PARAMETER, parameter),
                    new ServerActionParameter<String>(PARAM_EXPORTFORMAT, PARAM_EXPORTFORMAT_SHP),
                    new ServerActionParameter<String>(PARAM_NAME, "boris-shapexport")
                };

            // final ConsoleAppender consoleAppender = new ConsoleAppender();
            // consoleAppender.setThreshold(Priority.DEBUG);
            // BasicConfigurator.configure(consoleAppender);
            BasicConfigurator.configure();
            final BorisExportAction borisExportAction = new BorisExportAction();

            final Object result = borisExportAction.execute(null, serverActionParameters);
            // final Path csvFile = Files.write(Paths.get("boris-export.xlsx"), result.toString().getBytes("UTF-8"));

            final Path file = Files.write(Paths.get("boris-export.zip"), (byte[])result);
            System.out.println("Export File written to "
                        + file.toAbsolutePath().toString());
        } catch (Throwable ex) {
            Logger.getLogger(BorisExportAction.class).fatal(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
