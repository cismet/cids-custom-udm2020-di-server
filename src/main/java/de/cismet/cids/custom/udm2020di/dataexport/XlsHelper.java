/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.dataexport;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé <pascal.dihe@cismet.de>
 * @version  $Revision$, $Date$
 */
public class XlsHelper {

    //~ Instance fields --------------------------------------------------------

    protected final Logger log = Logger.getLogger(XlsHelper.class);

    protected String[] columnNames = null;
    protected final Map<String, Short> columnMap = new HashMap<String, Short>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new XlsHelper object.
     */
    public XlsHelper() {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param  headerRow  DOCUMENT ME!
     */
    public void initColumnMap(final Row headerRow) {
        if (!this.columnMap.isEmpty()) {
            log.warn("suspicious call to initColumnMap: columns aready processed!");
            this.columnMap.clear();
        }

        final short minColIx = headerRow.getFirstCellNum();
        final short maxColIx = headerRow.getLastCellNum();
        this.columnNames = new String[maxColIx + 1];
        for (short i = minColIx; i < maxColIx; i++) {
            final Cell cell = headerRow.getCell(i);
            if (cell == null) {
                log.warn("header cell #" + i + " is empty!");
            } else if (cell.getCellType() == Cell.CELL_TYPE_STRING) {
                final String columnName = cell.getStringCellValue();
                columnNames[i] = columnName;
                if (this.columnMap.containsKey(columnName)) {
                    log.warn("duplicate column name '" + columnName + "' for cells "
                                + this.columnMap.get(columnName) + " and " + i + '!');
                } else {
                    this.columnMap.put(columnName, i);
                }
            } else {
                log.warn("header cell #" + i + " is not of type STRING!");
            }
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   columnName  DOCUMENT ME!
     * @param   row         DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public Object getCellValue(final String columnName, final Row row) {
        if (this.columnMap.containsKey(columnName)) {
            final Cell cell = row.getCell(this.columnMap.get(columnName),
                    Row.RETURN_BLANK_AS_NULL);
            return this.getCellValue(cell);
        } else {
            log.warn("column '" + columnName + "' not found in sheet");
        }

        return null;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   cell  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public Object getCellValue(final Cell cell) {
        if (cell != null) {
            switch (cell.getCellType()) {
                case Cell.CELL_TYPE_BOOLEAN: {
                    return cell.getBooleanCellValue();
                }
                case Cell.CELL_TYPE_STRING: {
                    return cell.getStringCellValue();
                }
                case Cell.CELL_TYPE_NUMERIC: {
                    return cell.getNumericCellValue();
                }
                default: {
                    return null;
                }
            }
        }

        return null;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String[] getColumnNames() {
        return columnNames;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public Map<String, Short> getColumnMap() {
        return columnMap;
    }
}
