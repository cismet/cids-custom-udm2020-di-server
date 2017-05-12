/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions;

import com.vividsolutions.jts.geom.Geometry;

import oracle.jdbc.OracleConnection;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.deegree.datatypes.Types;
import org.deegree.datatypes.UnknownTypeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.text.NumberFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;

import de.cismet.cids.server.actions.ServerAction;

import de.cismet.cismap.commons.features.DefaultFeatureServiceFeature;
import de.cismet.cismap.commons.features.FeatureServiceFeature;
import de.cismet.cismap.commons.featureservice.DefaultLayerProperties;
import de.cismet.cismap.commons.featureservice.FeatureServiceAttribute;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public abstract class AbstractExportAction extends OracleExport implements ServerAction {

    //~ Static fields/initializers ---------------------------------------------

    public static final String PARAM_EXPORTFORMAT = "exportFormat";
    public static final String PARAM_EXPORTFORMAT_CSV = "CSV Datei";
    public static final String PARAM_EXPORTFORMAT_XLSX = "Excel Datei (XLSX)";
    public static final String PARAM_EXPORTFORMAT_XLS = "Excel Datei (XLS)";
    public static final String PARAM_EXPORTFORMAT_SHP = "ESRI Shape Datei";
    public static final String PARAM_NAME = "name";
    public static final String PARAM_PARAMETER = "parameter";
    public static final String PARAM_INTERNAL = "internal";

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AbstractExportAction object.
     */
    public AbstractExportAction() {
        super(false);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    protected boolean init(final InputStream propertyFile) throws IOException, ClassNotFoundException, SQLException {
        return super.init(propertyFile);
    }

    /**
     * DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected synchronized void checkConnection() throws SQLException {
        if ((this.sourceConnection == null) || (this.sourceConnection.pingDatabase() != OracleConnection.DATABASE_OK)) {
            final String sourceJdbcUrl = properties.getProperty("source.jdbc.url");
            final String sourceJdbcUsername = properties.getProperty("source.jdbc.username");
            final String sourceJdbcPassword = properties.getProperty("source.jdbc.password");
            final String sourceJdbcSchema = properties.getProperty("source.jdbc.schema");

            log.warn("Oracle Connection to '" + sourceJdbcUrl + "' lost, trying to reconnect");

            this.sourceConnection = createConnection(
                    sourceJdbcUrl,
                    sourceJdbcUsername,
                    sourceJdbcPassword,
                    sourceJdbcSchema);
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
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    public byte[] createXlsx(final ResultSet resultSet, final String name) throws SQLException, IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final Workbook workbook = new XSSFWorkbook();
        final Sheet exportSheet = workbook.createSheet(name);

        // 1st row: metadata
        final int columnCount = metaData.getColumnCount();
        int rowIndex = 0;
        Row row = exportSheet.createRow(rowIndex++);
        for (int i = 1; i <= columnCount; i++) {
            final String columnName = metaData.getColumnName(i);
            row.createCell(i - 1).setCellValue(columnName);
        }

        while (resultSet.next()) {
            row = exportSheet.createRow(rowIndex++);
            for (int i = 1; i <= columnCount; i++) {
//                if (log.isDebugEnabled()) {
//                    log.debug("column #" + i + " type = " + resultSet.getMetaData().getColumnTypeName(i));
//                }
                if ((resultSet.getMetaData().getScale(i) > 0)
                            || resultSet.getMetaData().getColumnTypeName(i).equals("NUMBER")) {
                    final double value = resultSet.getDouble(i);
                    final Cell cell = row.createCell(i - 1);
                    // final CellStyle style = workbook.createCellStyle();
                    // style.setDataFormat(workbook.createDataFormat().getFormat("#,############"));
                    // cell.setCellStyle(style);
                    cell.setCellValue(value);
                } else {
                    final String value = resultSet.getString(i);
                    if ((value != null) && (!value.isEmpty())) {
                        row.createCell(i - 1).setCellValue(value);
                    }
                }
            }
        }

        final byte[] bytes;
        try(final ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            workbook.write(output);
            bytes = output.toByteArray();
        }
        resultSet.close();

        log.info((rowIndex - 1) + " resources exported from Database and written to XSLX File.");
        return bytes;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   resultSet  DOCUMENT ME!
     * @param   name       DOCUMENT ME!
     * @param   zip        DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    public Object createCsv(final ResultSet resultSet, final String name, final boolean zip) throws SQLException,
        IOException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final StringBuilder csvBuilder = new StringBuilder();

        final int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            final String columnName = metaData.getColumnName(i);
            csvBuilder.append('\"');
            csvBuilder.append(columnName.replace('\"', '\'').replace('\n', ' '));
            csvBuilder.append('\"');

            if (i < columnCount) {
                csvBuilder.append(", ");
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("CSV Header: " + csvBuilder.toString());
        }
        csvBuilder.append(System.getProperty("line.separator"));

        int numResults = 0;
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                final String value;
                if ((resultSet.getMetaData().getScale(i) > 0)
                            || resultSet.getMetaData().getColumnTypeName(i).equals("NUMBER")) {
                    value = NumberFormat.getNumberInstance(Locale.GERMANY).format(resultSet.getDouble(i));
                } else {
                    value = resultSet.getString(i);
                }

                if ((value != null) && (value.length() > 0)) {
                    csvBuilder.append('\"');
                    csvBuilder.append(value.replace('\"', '\'').replace('\n', ' '));
                    csvBuilder.append('\"');
                }
                if (i < columnCount) {
                    csvBuilder.append(", ");
                }
            }
            csvBuilder.append(System.getProperty("line.separator"));
            numResults++;
        }

        log.info(numResults + " resources exported from Database");
        resultSet.close();

        if (zip) {
            if (log.isDebugEnabled()) {
                log.debug("zipping output");
            }
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            try(final ZipOutputStream zipStream = new ZipOutputStream(output)) {
                zipStream.putNextEntry(new ZipEntry(name));
                zipStream.write(csvBuilder.toString().getBytes("UTF-8"));
                zipStream.closeEntry();
            }

            return output.toByteArray();
        } else {
            return csvBuilder.toString();
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
     * @throws  Exception  DOCUMENT ME!
     */
    public abstract byte[] createShapeFile(final ResultSet resultSet, final String name) throws Exception;

    /**
     * DOCUMENT ME!
     *
     * @param   id           DOCUMENT ME!
     * @param   geometry     DOCUMENT ME!
     * @param   resultSet    DOCUMENT ME!
     * @param   metaData     DOCUMENT ME!
     * @param   columnCount  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected FeatureServiceFeature createFeatureServiceFeature(
            final int id,
            final Geometry geometry,
            final ResultSet resultSet,
            final ResultSetMetaData metaData,
            final int columnCount) throws SQLException {
        final FeatureServiceFeature feature = new DefaultFeatureServiceFeature(
                id,
                geometry,
                new DefaultLayerProperties());

        for (int columnNum = 1; columnNum <= columnCount; columnNum++) {
            final Object value;
            if ((resultSet.getMetaData().getScale(columnNum) > 0)
                        || resultSet.getMetaData().getColumnTypeName(columnNum).equals("NUMBER")) {
                // FIXME:
                // invalid data type at field: 3
                // at org.deegree.io.dbaseapi.DBFDataSection.setRecord(DBFDataSection.java:133)
                // when using other types than String!
                value = NumberFormat.getNumberInstance(Locale.GERMANY).format(resultSet.getDouble(columnNum));
            } else {
                value = resultSet.getString(columnNum);
            }
            // value = resultSet.getObject(columnNum);
            if (value != null) {
                // FIXME: DBF COLUMN NAME MADNESS!!!
                feature.addProperty(metaData.getColumnName(columnNum).replaceAll("\\.", new String()).replace(
                        ' ',
                        '_').trim(),
                    value);
            }
        }

        feature.addProperty("geom", geometry);
        return feature;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   metaData            DOCUMENT ME!
     * @param   columnCount         DOCUMENT ME!
     * @param   aliasAttributeList  DOCUMENT ME!
     * @param   propertyTypesMap    DOCUMENT ME!
     *
     * @throws  SQLException          DOCUMENT ME!
     * @throws  UnknownTypeException  DOCUMENT ME!
     */
    protected void fillPropertyTypesMap(
            final ResultSetMetaData metaData,
            final int columnCount,
            final List<String[]> aliasAttributeList,
            final Map<String, FeatureServiceAttribute> propertyTypesMap) throws SQLException, UnknownTypeException {
        final HashMap<String, Integer> dbfColumnNames = new HashMap<String, Integer>();
        final HashSet<String> renamedDbfColumnNames = new HashSet<String>();
        for (int columnNum = 1; columnNum <= columnCount; columnNum++) {
            String dbfColumnName = metaData.getColumnName(columnNum).replaceAll("\\.", new String()).replace(
                    ' ',
                    '_');

            if (dbfColumnName.length() > 10) {
                dbfColumnName = dbfColumnName.substring(0, 10).trim();
            }

            String renamedDbfColumnName = dbfColumnName;

            if (dbfColumnNames.containsKey(dbfColumnName)) {
                int i = dbfColumnNames.remove(dbfColumnName);
                i++;
                renamedDbfColumnName = dbfColumnName.substring(0, 8) + '_' + i;

                if (renamedDbfColumnNames.contains(renamedDbfColumnName)) {
                    i++;
                    renamedDbfColumnName = dbfColumnName.substring(0, 8) + '_' + i;
                    log.warn("duplicate duplicate DBF Column name '" + dbfColumnName + "' renamed to '"
                                + renamedDbfColumnName + "'");
                } else {
                    log.warn("duplicate DBF Column name '" + dbfColumnName + "' renamed to '"
                                + renamedDbfColumnName + "'");
                }

                renamedDbfColumnNames.add(renamedDbfColumnName);
                dbfColumnNames.put(dbfColumnName, i);
            } else {
                dbfColumnNames.put(dbfColumnName, 0);
            }

            aliasAttributeList.add(new String[] { renamedDbfColumnName, renamedDbfColumnName });

            propertyTypesMap.put(
                renamedDbfColumnName,
                new FeatureServiceAttribute(
                    renamedDbfColumnName,
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
    }
}
