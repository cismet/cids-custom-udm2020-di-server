/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.eprtr;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.indeximport.eprtr.EprtrImport;
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
public class EprtrExportAction extends AbstractExportAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String TASK_NAME = "eprtrExportAction";
    public static final String PARAM_INSTALLATIONS = "installations";
    public static final int EPSG = 4326;

    //~ Instance fields --------------------------------------------------------

    protected final String decodeSampleValuesStatementTpl;
    protected final String exportEprtrReleaseStatementTpl;
    protected final String projectionFile;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CsvExportAction object.
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public EprtrExportAction() throws IOException, ClassNotFoundException, SQLException {
        super(EprtrImport.class.getResourceAsStream("eprtr.properties"));
        this.log = Logger.getLogger(EprtrExportAction.class);
        log.info("new EprtrExportAction created");

        this.decodeSampleValuesStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/eprtr/decode-eprtr-releases.tpl.sql"),
                "UTF-8");

        this.exportEprtrReleaseStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/eprtr/export-eprtr-releases.tpl.sql"),
                "UTF-8");

        this.projectionFile = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/GCS_WGS_1984.prj"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   installationPks  DOCUMENT ME!
     * @param   parameters       DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createExportEprtrReleaseStatement(
            final Collection<Long> installationPks,
            final Collection<Parameter> parameters) {
        if (log.isDebugEnabled()) {
            log.debug("creating export statements for " + installationPks.size() + " installatione and "
                        + parameters.size() + parameters);
        }

        final StringBuilder installationeBuilder = new StringBuilder();
        final Iterator<Long> installationPksIterator = installationPks.iterator();
        while (installationPksIterator.hasNext()) {
            installationeBuilder.append(installationPksIterator.next());
            if (installationPksIterator.hasNext()) {
                installationeBuilder.append(',');
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

        String exportEprtrReleaseStatement = exportEprtrReleaseStatementTpl.replace(
                "%RELEASE_DECODE_STATEMENTS%",
                decodeBuilder);
        exportEprtrReleaseStatement = exportEprtrReleaseStatement.replace(
                "%RELEASE_PARAMETER_PKS%",
                parameterBuilder);
        exportEprtrReleaseStatement = exportEprtrReleaseStatement.replace(
                "%INSTALLATION_ERAS_IDS%",
                installationeBuilder);
        if (log.isDebugEnabled()) {
            log.debug(exportEprtrReleaseStatement);
        }
        return exportEprtrReleaseStatement;
    }

    @Override
    public Object execute(final Object body, final ServerActionParameter... params) {
        Statement exportEprtrReleaseStatement = null;
        ResultSet exportEprtrReleaseResult = null;
        try {
            this.checkConnection();

            Object result = null;

            Collection<Long> installationPks = null;
            Collection<Parameter> parameters = null;
            String exportFormat = PARAM_EXPORTFORMAT_CSV;
            String name = "export";

            for (final ServerActionParameter param : params) {
                if (param.getKey().equalsIgnoreCase(PARAM_INSTALLATIONS)) {
                    installationPks = (Collection<Long>)param.getValue();
                } else if (param.getKey().equalsIgnoreCase(PARAM_PARAMETER)) {
                    parameters = (Collection<Parameter>)param.getValue();
                } else if (param.getKey().equalsIgnoreCase(PARAM_EXPORTFORMAT)) {
                    exportFormat = param.getValue().toString();
                } else if (param.getKey().equalsIgnoreCase(PARAM_NAME)) {
                    name = param.getValue().toString();
                } else {
                    log.warn("ignoring unsupported server action parameter: '"
                                + param.getKey() + "' = '" + param.getValue() + "'!");
                }
            }

            if ((installationPks != null) && (parameters != null)) {
                log.info("performing '" + TASK_NAME + "' for " + installationPks.size()
                            + " EPRTR INSTALLATIONS and " + parameters.size() + " parameters to '"
                            + name + "' (" + exportFormat + ")");

                final String exportEprtrRelease = this.createExportEprtrReleaseStatement(installationPks, parameters);

                exportEprtrReleaseStatement = this.sourceConnection.createStatement();
                exportEprtrReleaseResult = exportEprtrReleaseStatement.executeQuery(exportEprtrRelease);

                if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_CSV)) {
                    result = this.createCsv(exportEprtrReleaseResult, name, false);
                } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_XLSX)) {
                    result = this.createXlsx(exportEprtrReleaseResult, name);
                } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_SHP)) {
                    result = this.createShapeFile(exportEprtrReleaseResult, name);
                } else {
                    final String message = "unsupported export format '" + exportFormat + "'";
                    log.error(message);
                    throw new Exception(message);
                }

                exportEprtrReleaseStatement.close();
            } else {
                log.error("no PARAM_INSTALLATIONS and PARAM_PARAMETER server action parameters provided,"
                            + "returning null");
            }

            return result;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            if (exportEprtrReleaseResult != null) {
                try {
                    exportEprtrReleaseResult.close();
                } catch (Exception e) {
                    log.error("could not close exportEprtrReleaseResult", e);
                }
            }

            if (exportEprtrReleaseStatement != null) {
                try {
                    exportEprtrReleaseStatement.close();
                } catch (Exception e) {
                    log.error("could not close exportEprtrReleaseStatement", e);
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
    protected byte[] createShapeFile(final ResultSet resultSet, final String name) throws SQLException,
        DBaseException,
        GeometryException,
        IOException,
        UnknownTypeException,
        Exception {
        final PrecisionModel precisionModel = new PrecisionModel(PrecisionModel.FLOATING);
        final GeometryFactory geometryFactory = new GeometryFactory(precisionModel, EPSG);
        final ResultSetMetaData metaData = resultSet.getMetaData();

        final int columnCount = metaData.getColumnCount();
        final List<String[]> aliasAttributeList = new ArrayList<String[]>();
        final Map<String, FeatureServiceAttribute> propertyTypesMap =
            new LinkedHashMap<String, FeatureServiceAttribute>();

        for (int columnNum = 1; columnNum <= columnCount; columnNum++) {
            aliasAttributeList.add(
                new String[] {
                    metaData.getColumnLabel(columnNum),
                    metaData.getColumnName(columnNum)
                });

            propertyTypesMap.put(
                metaData.getColumnName(columnNum),
                new FeatureServiceAttribute(
                    metaData.getColumnName(columnNum),
                    String.valueOf(Types.getTypeNameForSQLTypeCode(metaData.getColumnType(columnNum))),
                    true));
        }

        aliasAttributeList.add(new String[] { "geom", "geom" });
        propertyTypesMap.put(
            "geom",
            new FeatureServiceAttribute(
                "geom",
                String.valueOf(Types.GEOMETRY),
                true));

        final List<FeatureServiceFeature> featureList = new ArrayList<FeatureServiceFeature>();
        int rowNum = 0;

        while (resultSet.next()) {
            rowNum++;

            final float RECHTSWERT = resultSet.getFloat("LONGITUDE"); // Y
            final float HOCHWERT = resultSet.getFloat("LATITUDE");    // X
            final String INSTALLATION_ERAS_ID = resultSet.getString("INSTALLATION_ERAS_ID");

            int id;
            try {
                id = Integer.parseInt(INSTALLATION_ERAS_ID);
            } catch (Exception ex) {
                id = rowNum;
            }

            final Geometry geometry = geometryFactory.createPoint(new Coordinate(RECHTSWERT, HOCHWERT));
            final FeatureServiceFeature feature = new DefaultFeatureServiceFeature(
                    id,
                    geometry,
                    new DefaultLayerProperties());

            for (int columnNum = 1; columnNum <= columnCount; columnNum++) {
                feature.addProperty(metaData.getColumnName(columnNum), resultSet.getString(columnNum));
            }

            feature.addProperty("geom", geometry);
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
            final Collection<Long> installationPks = Arrays.asList(
                    new Long[] { 4263L, 4287L, 4498L, 4567L, 1183L, 1985L, 2103L, 4077L, 4144L });

            final Collection<Parameter> parameter = Arrays.asList(
                    new Parameter[] {
                        new Parameter("tscurwaxyd36cx", "Kohlenmonoxid (CO)"),
                        new Parameter("vqktwy5mugt4im", "Phenole (als Gesamt-C)"),
                        new Parameter("2wdixwiua7cjxm", "Stickoxide (NO/NO2)"),
                        new Parameter("yidbi85iuww432", "Ammoniak (NH3)"),
                        new Parameter("2ce2z9q9pv5j8w", "Quecksilber (Hg)"),
                        new Parameter("g6exbw2kyryag8", "Kupfer (Cu)")
                    });

            final ServerActionParameter[] serverActionParameters = new ServerActionParameter[] {
                    new ServerActionParameter<Collection<Long>>(PARAM_INSTALLATIONS, installationPks),
                    new ServerActionParameter<Collection<Parameter>>(PARAM_PARAMETER, parameter),
                    new ServerActionParameter<String>(PARAM_EXPORTFORMAT, PARAM_EXPORTFORMAT_SHP),
                    new ServerActionParameter<String>(PARAM_NAME, "eprtr-shp-export")
                };

            BasicConfigurator.configure();
            final EprtrExportAction eprtrExportAction = new EprtrExportAction();

            final Object result = eprtrExportAction.execute(null, serverActionParameters);
            // final Path csvFile = Files.write(Paths.get("eprtr-export.xlsx"), result.toString().getBytes("UTF-8"));

            final Path file = Files.write(Paths.get("eprtr-shp-export.zip"), (byte[])result);
            System.out.println("Export File written to "
                        + file.toAbsolutePath().toString());
        } catch (Throwable ex) {
            Logger.getLogger(EprtrExportAction.class).fatal(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
