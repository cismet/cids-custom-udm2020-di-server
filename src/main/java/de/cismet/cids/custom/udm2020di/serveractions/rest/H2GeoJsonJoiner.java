/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serveractions.rest;

import org.apache.log4j.Logger;

import org.h2gis.utilities.SFSUtilities;
import org.h2gis.utilities.wrapper.ConnectionWrapper;
import org.h2gis.utilities.wrapper.StatementWrapper;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import de.cismet.cids.custom.udm2020di.types.Parameter;

import de.cismet.tools.FileUtils;

/**
 * DOCUMENT ME!
 *
 * @author   therter, Pascal Dihé <pascal@cismet.de>
 * @version  $Revision$, $Date$
 */
public class H2GeoJsonJoiner {

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger LOG = Logger.getLogger(H2GeoJsonJoiner.class);
    private static final String SPATIAL_INIT = "CALL H2GIS_SPATIAL();";
    private static final String CREATE_SPATIAL_INIT_ALIAS =
        "CREATE ALIAS IF NOT EXISTS H2GIS_SPATIAL FOR \"org.h2gis.functions.factory.H2GISFunctions.load\";";
    private static final String TRANSFORM =
        "update %TABLE_NAME% set the_geom = st_transform(st_setsrid(the_geom, %MERGE_SRID%), %EXPORT_SRID%)";
    private static final String SET_SRID = "update %TABLE_NAME% set the_geom = st_setsrid(the_geom, %EXPORT_SRID%)";
    private static final String QUERY =
        "select %EXPORT_PARAMETERS% %MERGE_PARAMETERS% from %EXPORT_TABLE% export left join %MERGE_TABLE% merge on (export.the_geom && merge.the_geom and st_intersects(export.the_geom, merge.the_geom))";
    private static final String CREATE_SPATIAL_INDEX =
        "CREATE SPATIAL INDEX %INDEX_NAME% ON %TABLE_NAME% (%COLUMN_NAME%);";

    // Pfade und Tabellennamen eventuell aendern
    private static final String DB_NAME = "tmpDatabase";
    private static final String TABLE_NAME = "table";
    private static final HashSet<String> EXCLUDED_COLUMN_NAMES = new HashSet<String>();

    static {
        EXCLUDED_COLUMN_NAMES.add("PK");
        EXCLUDED_COLUMN_NAMES.add("THE_GEOM");
        EXCLUDED_COLUMN_NAMES.add("XKOORDINATE"); // WAGW_STATION
        EXCLUDED_COLUMN_NAMES.add("YKOORDINATE"); // WAGW_STATION
        EXCLUDED_COLUMN_NAMES.add("RECHTSWERT");  // BORIS_SITE
        EXCLUDED_COLUMN_NAMES.add("HOCHWERT");    // BORIS_SITE
    }

    //~ Instance fields --------------------------------------------------------

    private int tableCount = 0;
    private final ConnectionWrapper exportConnection;
    private final String dbPath;
    private final File exportDataShape;
    private final File mergeGeoJson;
    private final int exportCrs;
    private final int mergeCrs;
    private final Collection<String> exportParameters;
    private final Collection<Parameter> mergeParameters;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new H2GeoJsonJoiner object.
     *
     * @param   exportData       DOCUMENT ME!
     * @param   mergeData        DOCUMENT ME!
     * @param   mergeParameters  DOCUMENT ME!
     * @param   exportCrs        DOCUMENT ME!
     * @param   mergeCrs         DOCUMENT ME!
     *
     * @throws  IOException             DOCUMENT ME!
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    public H2GeoJsonJoiner(
            final byte[] exportData,
            final byte[] mergeData,
            final Collection<Parameter> mergeParameters,
            final int exportCrs,
            final int mergeCrs) throws IOException, ClassNotFoundException, SQLException {
        this.exportCrs = exportCrs;
        this.mergeCrs = mergeCrs;
        this.mergeParameters = mergeParameters;
        this.exportParameters = new ArrayList<String>();
        this.dbPath = createTempDirectory().getAbsolutePath();
        this.exportConnection = getDBConnection(dbPath + "/" + DB_NAME);
        this.exportDataShape = getUnzippedFileFromByteArray(exportData, "shp");
        this.mergeGeoJson = getUnzippedFileFromByteArray(mergeData, "geojson");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  IOException       DOCUMENT ME!
     * @throws  SQLException      DOCUMENT ME!
     * @throws  RuntimeException  DOCUMENT ME!
     */
    public ResultSet getResultSet() throws IOException, SQLException, RuntimeException {
        if (exportDataShape.isDirectory() || mergeGeoJson.isDirectory()) {
            final String message = "importing and merging '"
                        + exportDataShape.getAbsolutePath() + "' or '" + mergeGeoJson.getAbsolutePath()
                        + "' is a directory";
            LOG.error(message);
            throw new RuntimeException(message);
        }

        this.initDatabase(exportConnection);

        LOG.info("merging " + this.exportParameters.size()
                    + " properties of Shape file '" + exportDataShape.getName() + "' (EPSG:"
                    + this.exportCrs + ") with " + this.mergeParameters.size()
                    + "' properties of GeoJSON file '" + mergeGeoJson.getName() + "' (EPSG:"
                    + this.mergeCrs);

        final String exportDataTable = importShpFileToDb(
                exportConnection,
                exportDataShape.getAbsolutePath(),
                exportCrs);

        final String mergeDataTable = importGeoJsonFileToDb(
                exportConnection,
                mergeGeoJson.getAbsolutePath(),
                exportCrs,
                mergeCrs);
        final StatementWrapper st = createStatement(exportConnection);

        String query = QUERY.replace("%EXPORT_TABLE%", exportDataTable);
        query = query.replace("%MERGE_TABLE%", mergeDataTable);

        final StringBuilder exportParameterBuilder = new StringBuilder();
        final Iterator<String> exportParameterIterator = this.exportParameters.iterator();
        while (exportParameterIterator.hasNext()) {
            exportParameterBuilder.append("export.")
                    .append('\"')
                    .append(exportParameterIterator.next())
                    .append('\"')
                    .append(',');
        }

        final StringBuilder mergeParameterBuilder = new StringBuilder();
        final Iterator<Parameter> mergeParameterIterator = this.mergeParameters.iterator();
        while (mergeParameterIterator.hasNext()) {
            final Parameter parameter = mergeParameterIterator.next();

            mergeParameterBuilder.append("merge.").append('\"').append(parameter.getParameterName()).append('\"');
            // mergeParameterBuilder.append(" AS \'").append(parameter.getParameterName()).append('\'');
            if (mergeParameterIterator.hasNext()) {
                mergeParameterBuilder.append(',');
            }
        }

        query = query.replace("%EXPORT_PARAMETERS%", exportParameterBuilder.toString());
        query = query.replace("%MERGE_PARAMETERS%", mergeParameterBuilder.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug(query);
        }

        final ResultSet rs = st.executeQuery(query);
        return rs;
    }

    /**
     * Performs Cleanup of connections and deletes temporary files.
     */
    public void close() {
        try {
            exportConnection.close();
            deleteDirectory(new File(dbPath));
        } catch (SQLException e) {
            LOG.error("Error while closing db connection: " + e.getMessage(), e);
        }
        deleteDirectory(exportDataShape.getParentFile());
        deleteDirectory(mergeGeoJson.getParentFile());
    }

    /**
     * Imports the to-be-merged GeoJson file into the DB and transforms the SRID of the geoJson geometries (in general
     * 4326) to the SRID of the Export Shape File.
     *
     * @param   connectionWrapper  con DOCUMENT ME!
     * @param   geojsonFile        shpFile DOCUMENT ME!
     * @param   exportSrid         DOCUMENT ME!
     * @param   mergeSrid          DOCUMENT ME!
     *
     * @return  the name of the table, that contains the given geojson file
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private String importGeoJsonFileToDb(
            final ConnectionWrapper connectionWrapper,
            final String geojsonFile,
            final int exportSrid,
            final int mergeSrid) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("importing geojson File for merging: '" + geojsonFile + "'.");
        }

        final String table = TABLE_NAME + "_" + (++tableCount);
        try(final StatementWrapper statementWrapper = createStatement(connectionWrapper)) {
            statementWrapper.execute("CALL GeoJsonRead('" + geojsonFile + "', '" + table + "');");

            if (mergeSrid != exportSrid) {
                LOG.warn("transforming geoJson merge SRID '" + mergeSrid + "' to SHape EXPORT SRID '" + exportSrid
                            + "'");
                String update = TRANSFORM.replace("%TABLE_NAME%", table);
                update = update.replace("%MERGE_SRID%", String.valueOf(mergeSrid));
                update = update.replace("%EXPORT_SRID%", String.valueOf(exportSrid));
                statementWrapper.execute(update);
            }
        }

        createSpatialIndex("the_geom", table);

        return table;
    }

    /**
     * Imports the Export Shape file that is to-be merged with the GeoJson File provided by the user to the temporary H2
     * DB. The Export Shape is generated by the OracleExports and contains only Point Features.
     *
     * @param   connectionWrapper  DOCUMENT ME!
     * @param   shpFile            DOCUMENT ME!
     * @param   exportSrid         DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private String importShpFileToDb(
            final ConnectionWrapper connectionWrapper,
            final String shpFile,
            final int exportSrid) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("importing Shape File for merging: '" + shpFile + "'.");
        }

        final String table = TABLE_NAME + "_" + (++tableCount);
        final String update = SET_SRID.replace("%TABLE_NAME%", table)
                    .replace("%EXPORT_SRID%", String.valueOf(exportSrid));
        try(final StatementWrapper statementWrapper = createStatement(connectionWrapper)) {
            statementWrapper.execute("CALL SHPREAD('" + shpFile + "', '" + table + "');");
            statementWrapper.execute(update);
        }

        this.createSpatialIndex("the_geom", table);
        this.exportParameters.addAll(this.getColumnNames(connectionWrapper, table, EXCLUDED_COLUMN_NAMES));

        return table;
    }

    /**
     * creates a spatial index for the given field.
     *
     * @param   geoField   DOCUMENT ME!
     * @param   tableName  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private void createSpatialIndex(final String geoField, final String tableName) throws SQLException {
        try(final StatementWrapper st = createStatement(exportConnection)) {
            final String indexName = geoField + tableName + "SpatialIndex";
            String update = CREATE_SPATIAL_INDEX.replace("%INDEX_NAME%", indexName);
            update = update.replace("%TABLE_NAME%", tableName);
            update = update.replace("%COLUMN_NAME%", geoField);
            st.execute(update);
        } catch (SQLException e) {
            LOG.error("could not create spatial index: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Unzips the zip file in the given byte array.
     *
     * @param   array      DOCUMENT ME!
     * @param   extension  DOCUMENT ME!
     *
     * @return  the unzipped shape file
     *
     * @throws  IOException  DOCUMENT ME!
     */
    private File getUnzippedFileFromByteArray(final byte[] array, final String extension) throws IOException {
        try(final ZipInputStream zstream = new ZipInputStream(new ByteArrayInputStream(array))) {
            ZipEntry entry;
            final byte[] tmp = new byte[256];
            final File directory = createTempDirectory();

            while ((entry = zstream.getNextEntry()) != null) {
                final File file = new File(directory, entry.getName());
                final BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
                int length;

                try {
                    while ((length = zstream.read(tmp, 0, tmp.length)) != -1) {
                        out.write(tmp, 0, length);
                    }
                } finally {
                    out.close();
                }
            }

            final File[] unzippedFiles = directory.listFiles();

            if ((unzippedFiles != null) && (unzippedFiles.length > 0)) {
                for (final File file : unzippedFiles) {
                    if (FileUtils.getExt(file).equalsIgnoreCase(extension)) {
                        return file;
                    }
                }
            }

            LOG.warn("ZIP file does not contain *." + extension);
            return directory;
        }
    }

    /**
     * creates a temporary directory.
     *
     * @return  DOCUMENT ME!
     *
     * @throws  IOException  DOCUMENT ME!
     */
    private static File createTempDirectory() throws IOException {
        final File temp;
        int i = 0;

        temp = File.createTempFile("UDM2020-DI-EXPORT", Long.toString(System.nanoTime()));
        temp.delete();
        File directory = new File(temp.getAbsolutePath() + (++i));

        while (directory.exists()) {
            directory = new File(temp.getAbsolutePath() + (++i));
        }

        directory.mkdir();

        return directory;
    }

    /**
     * Deletes the given directory and its content.
     *
     * @param  dir  the directory to delete
     */
    private static void deleteDirectory(final File dir) {
        if ((dir != null)) {
            if ((dir.listFiles() != null)) {
                for (final File f : dir.listFiles()) {
                    if (f.isDirectory()) {
                        deleteDirectory(f);
                    } else {
                        f.delete();
                    }
                }
            }
            dir.delete();
        }
    }

    /*public static void main(final String[] args) {
     *  try {     final String smuFile = "/home/therter/Downloads/geojson-neues-format/FAO_Bodentypen.zip.geojson.zip";
     * final String exportFile = "/home/therter/Downloads/geojson-neues-format/Standorte.zip.geojson.zip";
     *
     * final H2GeoJsonJoiner i = new H2GeoJsonJoiner(FileAsByteArray(new File(exportFile)), FileAsByteArray(new
     * File(smuFile)),             4326,             31287);     i.getResultSet();     i.close(); }
     * catch (Exception e) {     LOG.error("error", e); }}*/

    /**
     * DOCUMENT ME!
     *
     * @param   file  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    private static byte[] FileAsByteArray(final File file) throws Exception {
        final BufferedInputStream is = new BufferedInputStream(new FileInputStream(file));
        final ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
        final BufferedOutputStream os = new BufferedOutputStream(byteArrayStream);
        final byte[] tmp = new byte[256];
        int length;

        try {
            while ((length = is.read(tmp, 0, tmp.length)) != -1) {
                os.write(tmp, 0, length);
            }
        } finally {
            is.close();
            os.close();
        }

        return byteArrayStream.toByteArray();
    }

    /**
     * DOCUMENT ME!
     *
     * @param   conn  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private StatementWrapper createStatement(final ConnectionWrapper conn) throws SQLException {
        return (StatementWrapper)conn.createStatement();
    }

    /**
     * DOCUMENT ME!
     *
     * @param   databasePath  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  ClassNotFoundException  DOCUMENT ME!
     * @throws  SQLException            DOCUMENT ME!
     */
    private ConnectionWrapper getDBConnection(final String databasePath) throws ClassNotFoundException, SQLException {
        final String jdbcUrl = "jdbc:h2:" + databasePath;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("creating database connection: " + jdbcUrl);
            }
            Class.forName("org.h2.Driver");
            return (ConnectionWrapper)SFSUtilities.wrapConnection(DriverManager.getConnection(jdbcUrl));
        } catch (ClassNotFoundException | SQLException e) {
            LOG.error("Error while creating database connection: '" + jdbcUrl
                        + "': " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Add spatial extensions (H2Gis) to the database.
     *
     * @param   connectionWrapper  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private void initDatabase(final ConnectionWrapper connectionWrapper) throws SQLException {
        try(final ResultSet rs = connectionWrapper.getMetaData().getTables(null, null, "GEOMETRY_COLUMNS", null)) {
            if (!rs.next()) {
                try(final StatementWrapper st = createStatement(connectionWrapper)) {
                    st.execute(
                        CREATE_SPATIAL_INIT_ALIAS);
                    st.execute(SPATIAL_INIT);
                }
            }
        } catch (SQLException e) {
            LOG.error("could not init database: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   connectionWrapper  DOCUMENT ME!
     * @param   tableName          DOCUMENT ME!
     * @param   excludedColumns    DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private Collection<String> getColumnNames(
            final ConnectionWrapper connectionWrapper,
            final String tableName,
            final HashSet<String> excludedColumns) throws SQLException {
        final ArrayList<String> columnNames = new ArrayList<String>();
        try {
            final ResultSet tableMetaData = connectionWrapper.getMetaData()
                        .getColumns(null, null, tableName.toUpperCase(), "%");
            while (tableMetaData.next()) {
                final String columnName = tableMetaData.getString(4);
                if (!excludedColumns.contains(columnName.toUpperCase())) {
                    columnNames.add(columnName);
                }
            }
            return columnNames;
        } catch (SQLException e) {
            LOG.error("could not get column names from table '" + tableName + "': " + e.getMessage(), e);
            throw e;
        }
    }
}
