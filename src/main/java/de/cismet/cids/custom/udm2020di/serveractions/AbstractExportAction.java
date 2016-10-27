/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions;

import oracle.jdbc.OracleConnection;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;

import de.cismet.cids.server.actions.ServerAction;

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
     *
     * @param   propertyFile  DOCUMENT ME!
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public AbstractExportAction(final InputStream propertyFile) throws IOException,
        ClassNotFoundException,
        SQLException {
        super(propertyFile, false);
    }

    //~ Methods ----------------------------------------------------------------

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
    protected byte[] createXlsx(final ResultSet resultSet, final String name) throws SQLException, IOException {
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
                final String value = resultSet.getString(i);
                if ((value != null) && (!value.isEmpty())) {
                    row.createCell(i - 1).setCellValue(value);
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
    protected Object createCsv(final ResultSet resultSet, final String name, final boolean zip) throws SQLException,
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
                final String value = resultSet.getString(i);
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
}
