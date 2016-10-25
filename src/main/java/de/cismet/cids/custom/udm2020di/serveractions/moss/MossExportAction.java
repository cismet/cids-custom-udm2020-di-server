/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.moss;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.poi.hssf.usermodel.HSSFCellStyle;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.deegree.datatypes.Types;
import org.deegree.datatypes.UnknownTypeException;
import org.deegree.io.dbaseapi.DBaseException;
import org.deegree.model.feature.FeatureCollection;
import org.deegree.model.spatialschema.GeometryException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.dataexport.XlsHelper;
import de.cismet.cids.custom.udm2020di.indeximport.moss.MossImport;
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

import static de.cismet.cids.custom.udm2020di.indeximport.moss.MossImport.DEFAULT_IMPORTFILE;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@org.openide.util.lookup.ServiceProvider(service = ServerAction.class)
public class MossExportAction extends AbstractExportAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String TASK_NAME = "mossExportAction";
    public static final String PARAM_OBJECT_IDS = "objectIds";
    public static final String PARAM_SAMPLE_IDS = "sampleIds";
    public static final int EPSG = 4326;

    //~ Instance fields --------------------------------------------------------

    protected final String decodeSampleValuesStatementTpl;
    protected final String exportMossStatementTpl;
    protected final String projectionFile;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new CsvExportAction object.
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public MossExportAction() throws IOException, ClassNotFoundException, SQLException {
        super(MossExportAction.class.getResourceAsStream(
                "/de/cismet/cids/custom/udm2020di/indeximport/moss/moss.properties"));
        this.log = Logger.getLogger(MossExportAction.class);
        log.info("new MossExportAction created");

        this.decodeSampleValuesStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/moss/decode-moss-samples.tpl.sql"),
                "UTF-8");

        this.exportMossStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/moss/export-moss-samples.tpl.sql"),
                "UTF-8");

        this.projectionFile = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/dataexport/GCS_WGS_1984.prj"),
                "UTF-8");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   sitePks     DOCUMENT ME!
     * @param   parameters  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createExportMossStatement(
            final Collection<Long> sitePks,
            final Collection<Parameter> parameters) {
        if (log.isDebugEnabled()) {
            log.debug("creating export statements for " + sitePks.size() + " MOSS Sites and "
                        + parameters.size() + " parameters");
        }

        final StringBuilder mosseBuilder = new StringBuilder();
        final Iterator<Long> mossPksIterator = sitePks.iterator();
        while (mossPksIterator.hasNext()) {
            mosseBuilder.append(mossPksIterator.next());
            if (mossPksIterator.hasNext()) {
                mosseBuilder.append(',');
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

        String exportMossMesswerteStatement = exportMossStatementTpl.replace(
                "%MOSS_DECODE_STATEMENTS%",
                decodeBuilder);
        exportMossMesswerteStatement = exportMossMesswerteStatement.replace(
                "%PARAMETER_PKS%",
                parameterBuilder);
        exportMossMesswerteStatement = exportMossMesswerteStatement.replace("%MOSS_IDS%", mosseBuilder);
        if (log.isDebugEnabled()) {
            log.debug(exportMossMesswerteStatement);
        }
        return exportMossMesswerteStatement;
    }

    @Override
    public Object execute(final Object body, final ServerActionParameter... params) {
        Statement exportMossStatement = null;
        ResultSet exportMossResult = null;
        try {
            this.checkConnection();

            Object result = null;

            Collection<Long> objectIds = null;
            Collection<String> sampleIds = null;
            Collection<Parameter> parameters = null;
            String exportFormat = PARAM_EXPORTFORMAT_CSV;
            String name = "export";

            for (final ServerActionParameter param : params) {
                if (param.getKey().equalsIgnoreCase(PARAM_OBJECT_IDS)) {
                    objectIds = (Collection<Long>)param.getValue();
                } else if (param.getKey().equalsIgnoreCase(PARAM_SAMPLE_IDS)) {
                    sampleIds = (Collection<String>)param.getValue();
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

            if ((parameters != null)) {
                if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_XLS)) {
                    if ((sampleIds != null) && !sampleIds.isEmpty()) {
                        log.info("performing '" + TASK_NAME + "' for " + sampleIds.size()
                                    + " MOSS Samples and " + parameters.size() + " parameters to '"
                                    + name + "' (" + exportFormat + ")");

                        final Collection<String> parameterPks = new HashSet();
                        for (final Parameter parameter : parameters) {
                            parameterPks.add(parameter.getParameterPk());
                        }

                        result = this.createXlsFile(sampleIds, parameterPks);
                    } else {
                        log.error("no PARAM_SAMPLE_IDS server action parameters provided,"
                                    + "returning null");
                    }
                } else if ((objectIds != null)) {
                    log.info("performing '" + TASK_NAME + "' for " + objectIds.size()
                                + " MOSS Objects and " + parameters.size() + " parameters to '"
                                + name + "' (" + exportFormat + ")");

                    final String exportMoss = this.createExportMossStatement(objectIds, parameters);
                    exportMossStatement = this.sourceConnection.createStatement();
                    exportMossResult = exportMossStatement.executeQuery(exportMoss);

                    if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_CSV)) {
                        result = this.createCsv(exportMossResult, name, false);
                    } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_XLSX)) {
                        result = this.createXlsx(exportMossResult, name);
                    } else if (exportFormat.equalsIgnoreCase(PARAM_EXPORTFORMAT_SHP)) {
                        result = this.createShapeFile(exportMossResult, name);
                    } else {
                        final String message = "unsupported export format '" + exportFormat + "'";
                        log.error(message);
                        throw new Exception(message);
                    }

                    exportMossStatement.close();
                } else {
                    log.error("no PARAM_OBJECT_IDS server action parameters provided,"
                                + "returning null");
                }
            } else {
                log.error("no PARAM_PARAMETERS server action parameters provided,"
                            + "returning null");
            }

            return result;
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
            if (exportMossResult != null) {
                try {
                    exportMossResult.close();
                } catch (Exception e) {
                    log.error("could not close exportMossResult", e);
                }
            }

            if (exportMossStatement != null) {
                try {
                    exportMossStatement.close();
                } catch (Exception e) {
                    log.error("could not close exportMossStatement", e);
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

            final float RECHTSWERT = resultSet.getFloat("XCOORDINATE"); // X
            final float HOCHWERT = resultSet.getFloat("YCOORDINATE");   // Y
            final int id = rowNum;

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

    /**
     * DOCUMENT ME!
     *
     * @param   sampleIds     DOCUMENT ME!
     * @param   parameterPks  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  InvalidFormatException  DOCUMENT ME!
     */
    protected byte[] createXlsFile(
            final Collection<String> sampleIds,
            final Collection<String> parameterPks) throws IOException, InvalidFormatException {
        final XlsHelper xlsHelper = new XlsHelper();
        final InputStream inputStream = MossImport.class.getResourceAsStream(DEFAULT_IMPORTFILE);
        final Workbook workbook = WorkbookFactory.create(inputStream);

        final CellStyle highlightColumnStyle = workbook.createCellStyle();
        highlightColumnStyle.setFillForegroundColor(HSSFColor.LIGHT_GREEN.index);
        highlightColumnStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);

        final CellStyle highlightRowStyle = workbook.createCellStyle();
        highlightRowStyle.setFillForegroundColor(HSSFColor.RED.index);
        highlightRowStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);

        final CellStyle highlightCellStyle = workbook.createCellStyle();
        highlightCellStyle.setFillForegroundColor(HSSFColor.YELLOW.index);
        highlightCellStyle.setFillPattern(HSSFCellStyle.SOLID_FOREGROUND);

        final Set<Short> highlightedColumns = new HashSet<Short>();

        if (log.isDebugEnabled()) {
            log.debug("reading XLS sheet '" + workbook.getSheetName(0)
                        + "' from workbook with " + workbook.getNumberOfSheets() + " sheets");
        }
        final Sheet mossSheet = workbook.getSheetAt(0);

        final int rows = mossSheet.getLastRowNum() + 1;
        log.info("processing Moss stations from XLS Sheet " + mossSheet.getSheetName()
                    + " with " + rows + " rows.");

        short rowIdx = 0;
        final Iterator<Row> rowIterator = mossSheet.rowIterator();
        while (rowIterator.hasNext()) {
            try {
                final Row row = rowIterator.next();
                ++rowIdx;

                // process cells of rows
                final short minColIx = row.getFirstCellNum();
                final short maxColIx = row.getLastCellNum();
                short columnIndex;

                // process first row and parse header information
                if (xlsHelper.getColumnMap().isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("reading sheet header information");
                    }
                    xlsHelper.initColumnMap(row);
                    log.info("sheet header information processed, " + xlsHelper.getColumnMap().size()
                                + " columns identified");

                    // process heder and identify columsn for highlighting
                    for (columnIndex = minColIx; columnIndex < maxColIx; columnIndex++) {
                        final String columnName = xlsHelper.getColumnNames()[columnIndex];
                        if ((columnName != null) && (columnName.indexOf('_') > 0)) {
                            final String potentialPollutantKey = columnName.substring(0, columnName.indexOf('_'));
                            if (parameterPks.contains(potentialPollutantKey)) {
                                if (log.isDebugEnabled()) {
                                    log.debug("highlighting column #" + columnIndex + " '" + columnName
                                                + "' with pollutant key " + potentialPollutantKey);
                                }
                                highlightedColumns.add(columnIndex);
                            }
                        }
                    }
                    continue;
                }

                boolean highlightFullRow = false;
                for (columnIndex = minColIx; columnIndex < maxColIx; columnIndex++) {
                    final Cell cell = row.getCell(columnIndex);
                    final boolean highlightFullColumn = highlightedColumns.contains(columnIndex);
                    // empty cell -> ignore
                    if (cell == null) {
                        if (log.isDebugEnabled()) {
                            log.warn("empty cell #" + columnIndex + " '"
                                        + xlsHelper.getColumnNames()[columnIndex] + "' of row #" + row.getRowNum());
                        }
                        continue;
                    }

                    // check first column and conditionally highlight sample row
                    if (columnIndex == minColIx) {
                        final Object sampleId = xlsHelper.getCellValue(cell);
                        if (sampleIds.contains(sampleId)) {
                            if (log.isDebugEnabled()) {
                                log.debug("highlighting row " + rowIdx + " with sample value id " + sampleId);
                            }
                            highlightFullRow = true;
                            cell.setCellStyle(highlightRowStyle);
                        }
                    } else if (highlightFullRow && highlightFullColumn) {
                        // cell and row ->  highlight value
                        cell.setCellStyle(highlightCellStyle);
                    } else if (highlightFullRow) {
                        cell.setCellStyle(highlightRowStyle);
                    } else if (highlightFullColumn) {
                        cell.setCellStyle(highlightColumnStyle);
                    }
                    // else: ignore
                }
            } catch (Throwable t) {
                log.error("could not process row " + rowIdx
                            + " due to error: " + t.getMessage(), t);
            }
        }

        inputStream.close();
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        workbook.write(output);
        output.close();
        return output.toByteArray();
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
            final Collection<Long> objectIds = Arrays.asList(
                    new Long[] {
                        1063L,
                        1075L,
                        1094L,
                        1105L,
                        1106L
                    });

            final Collection<String> sampleIds = Arrays.asList(
                    new String[] {
                        "213-1",
                        "33-1",
                        "50-1",
                        "60-1",
                        "60-2",
                    });

            final Collection<Parameter> parameter = Arrays.asList(
                    new Parameter[] {
                        new Parameter("Al", "Al [mg/kg]"),
                        new Parameter("As", "AS [mg/kg]"),
                        new Parameter("Zn", "Cd [mg/kg]")
                    });

            final ServerActionParameter[] serverActionParameters = new ServerActionParameter[] {
                    new ServerActionParameter<Collection<Long>>(PARAM_OBJECT_IDS, objectIds),
                    new ServerActionParameter<Collection<String>>(PARAM_SAMPLE_IDS, sampleIds),
                    new ServerActionParameter<Collection<Parameter>>(PARAM_PARAMETER, parameter),
                    new ServerActionParameter<String>(PARAM_EXPORTFORMAT, PARAM_EXPORTFORMAT_XLS),
                    new ServerActionParameter<String>(PARAM_NAME, "moss-xls-export")
                };

            BasicConfigurator.configure();

            final MossExportAction mossExportAction = new MossExportAction();

            final Object result = mossExportAction.execute(null, serverActionParameters);
            final Path file = Files.write(Paths.get("moss-export.xls"), (byte[])result);

            // final Path file = Files.write(Paths.get("moss-shp-export.zip"), (byte[])result);
            System.out.println("Export File written to "
                        + file.toAbsolutePath().toString());
        } catch (Throwable ex) {
            Logger.getLogger(MossExportAction.class).fatal(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
