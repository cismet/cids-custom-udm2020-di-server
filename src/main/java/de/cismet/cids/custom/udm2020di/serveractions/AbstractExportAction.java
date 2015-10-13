/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.cismet.cids.custom.udm2020di.serveractions;

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
    public static final String PARAM_EXPORTFORMAT_CSV = "exportFormatCsv";
    public static final String PARAM_EXPORTFORMAT_XLSX = "exportFormatXlsx";
    public static final String PARAM_EXPORTFORMAT_SHP = "exportFormatShp";
    public static final String PARAM_NAME = "name";
    public static final String PARAM_PARAMETER = "parameter";

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
     * @param   resultSet  DOCUMENT ME!
     * @param   name       DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    protected Object createXlsx(final ResultSet resultSet, final String name) throws SQLException, IOException {
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

        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        workbook.write(output);
        final byte[] bytes = output.toByteArray();

        output.close();
        resultSet.close();

        log.info((rowIndex - 1) + " resources exported from Database.");
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
            final ZipOutputStream zipStream = new ZipOutputStream(output);
            zipStream.putNextEntry(new ZipEntry(name));
            zipStream.write(csvBuilder.toString().getBytes("UTF-8"));
            zipStream.closeEntry();
            zipStream.close();

            return output.toByteArray();
        } else {
            return csvBuilder.toString();
        }
    }
}
