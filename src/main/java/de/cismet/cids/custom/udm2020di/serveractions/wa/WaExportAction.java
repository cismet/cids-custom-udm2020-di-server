/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.wa;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import org.apache.commons.io.IOUtils;

import org.deegree.datatypes.Types;
import org.deegree.datatypes.UnknownTypeException;
import org.deegree.io.dbaseapi.DBaseException;
import org.deegree.model.feature.FeatureCollection;
import org.deegree.model.spatialschema.GeometryException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.indeximport.wa.WaImport;
import de.cismet.cids.custom.udm2020di.serveractions.AbstractExportAction;
import de.cismet.cids.custom.udm2020di.types.Parameter;

import de.cismet.cids.server.actions.ServerActionParameter;

import de.cismet.cismap.commons.features.DefaultFeatureServiceFeature;
import de.cismet.cismap.commons.features.FeatureServiceFeature;
import de.cismet.cismap.commons.featureservice.DefaultLayerProperties;
import de.cismet.cismap.commons.featureservice.FeatureServiceAttribute;
import de.cismet.cismap.commons.gui.shapeexport.ShapeExportHelper;
import de.cismet.cismap.commons.tools.SimpleFeatureCollection;

import static de.cismet.cids.custom.udm2020di.serveractions.eprtr.EprtrExportAction.TASK_NAME;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public abstract class WaExportAction extends AbstractExportAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String WAOW = WaImport.WAOW;
    public static final String WAGW = WaImport.WAGW;

    public static final String PARAM_MESSSTELLEN = "messstellen";
    public static final int WA_EPSG = 31287;

    //~ Instance fields --------------------------------------------------------

    protected final String decodeSampleValuesStatementTpl;
    protected final String exportWaMesswerteStatementTpl;
    protected final String exportWaMesswerteStatementToShapefileTpl;
    protected final String projectionFile;
    protected final String waSource;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CsvExportAction object.
     *
     * @param   waSource  DOCUMENT ME!
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public WaExportAction(final String waSource) throws IOException, ClassNotFoundException, SQLException {
        super(WaImport.class.getResourceAsStream(waSource + ".properties"));
        this.waSource = waSource;

        this.decodeSampleValuesStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/"
                            + waSource
                            + "/decode-"
                            + waSource
                            + "-messwerte.tpl.sql"),
                "UTF-8");

        this.exportWaMesswerteStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/"
                            + waSource
                            + "/export-"
                            + waSource
                            + "-messwerte.tpl.sql"),
                "UTF-8");

        this.exportWaMesswerteStatementToShapefileTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/"
                            + waSource
                            + "/export-"
                            + waSource
                            + "-messwerte-to-shapefile.tpl.sql"),
                "UTF-8");

        this.projectionFile = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/MGI_Austria_Lambert.prj"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   exportTpl      DOCUMENT ME!
     * @param   messstellePks  DOCUMENT ME!
     * @param   parameters     DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createExportWaMesswerteStatement(
            final String exportTpl,
            final Collection<String> messstellePks,
            final Collection<Parameter> parameters) {
        if (log.isDebugEnabled()) {
            log.debug("creating export statements for " + messstellePks.size() + " Messstellen and "
                        + parameters.size() + " parameters.");
        }

        final StringBuilder messstelleeBuilder = new StringBuilder();
        final Iterator<String> messstellePksIterator = messstellePks.iterator();
        while (messstellePksIterator.hasNext()) {
            messstelleeBuilder.append('\'').append(messstellePksIterator.next()).append('\'');
            if (messstellePksIterator.hasNext()) {
                messstelleeBuilder.append(',');
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
            // ORACLE: column name length restriction
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

        String exportWaMesswerteStatement = exportTpl.replace(
                "%MESSWERT_DECODE_STATEMENTS%",
                decodeBuilder);
        exportWaMesswerteStatement = exportWaMesswerteStatement.replace(
                "%MESSWERT_PARAMETER_PKS%",
                parameterBuilder);
        exportWaMesswerteStatement = exportWaMesswerteStatement.replace("%MESSSTELLE_PKS%", messstelleeBuilder);
        if (log.isDebugEnabled()) {
            log.debug(exportWaMesswerteStatement);
        }
        return exportWaMesswerteStatement;
    }

    @Override
    public Object execute(final Object body, final ServerActionParameter... params) {
        Statement exportWaMesswerteStatement = null;
        ResultSet exportWaMesswerteResult = null;
        try {
            this.checkConnection();

            Object result = null;

            Collection<String> messstellePks = null;
            Collection<Parameter> parameters = null;
            String exportFormat = PARAM_EXPORTFORMAT_CSV;
            String name = "export";
            boolean isInternal = false;

            for (final ServerActionParameter param : params) {
                if (param.getKey().equalsIgnoreCase(PARAM_MESSSTELLEN)) {
                    messstellePks = (Collection<String>)param.getValue();
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

            if ((messstellePks != null) && (parameters != null)) {
                log.info("performing " + ((isInternal == true) ? "INTERNAL '" : "'") + TASK_NAME + "' for "
                            + messstellePks.size()
                            + " WAxW Stations and " + parameters.size() + " parameters to '"
                            + name + "' (" + exportFormat + ")");

                final String exportTpl = exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_SHP)
                    ? this.exportWaMesswerteStatementToShapefileTpl : this.exportWaMesswerteStatementTpl;
                final String exportWaMesswerte = this.createExportWaMesswerteStatement(
                        exportTpl,
                        messstellePks,
                        parameters);

                exportWaMesswerteStatement = this.sourceConnection.createStatement();
                exportWaMesswerteResult = exportWaMesswerteStatement.executeQuery(exportWaMesswerte);

                if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_CSV)) {
                    result = this.createCsv(exportWaMesswerteResult, name, false);
                } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_XLSX)) {
                    result = this.createXlsx(exportWaMesswerteResult, name);
                } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_SHP)) {
                    if (waSource.equalsIgnoreCase(WAOW)) {
                        result = this.createShapeFile(exportWaMesswerteResult, name);
                    } else {
                        if (isInternal == true) {
                            log.warn("performing INTERNAL SHP Export of WAGW Stations!");
                            result = this.createShapeFile(exportWaMesswerteResult, name);
                        } else {
                            final String message = "SHP Export of WAGW Stations not permitted! (" + exportFormat + ")";
                            log.error(message);
                            throw new Exception(message);
                        }
                    }
                } else {
                    final String message = "unsupported export format '" + exportFormat + "'";
                    log.error(message);
                    throw new Exception(message);
                }

                exportWaMesswerteStatement.close();
            } else {
                log.error("no PARAM_MESSSTELLEN and PARAM_PARAMETER server action parameters provided,"
                            + "returning null");
            }

            return result;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            if (exportWaMesswerteResult != null) {
                try {
                    exportWaMesswerteResult.close();
                } catch (Exception e) {
                    log.error("could not close exportWaMesswerteResult", e);
                }
            }

            if (exportWaMesswerteStatement != null) {
                try {
                    exportWaMesswerteStatement.close();
                } catch (Exception e) {
                    log.error("could not close exportWaMesswerteStatement", e);
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
        final GeometryFactory geometryFactory = new GeometryFactory(precisionModel, WA_EPSG);
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

            // final String STANDORT_PK = resultSet.getString("STANDORT_PK");
            final float RECHTSWERT = resultSet.getFloat("XKOORDINATE");
            final float HOCHWERT = resultSet.getFloat("YKOORDINATE");
            final String PROBE_PK = resultSet.getString("PROBE_PK");

            int id;
            try {
                id = Integer.parseInt(PROBE_PK);
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
}
