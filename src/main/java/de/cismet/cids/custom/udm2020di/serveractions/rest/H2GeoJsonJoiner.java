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

import java.util.Collection;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import de.cismet.cids.custom.udm2020di.types.Parameter;

/**
 * DOCUMENT ME!
 *
 * @author   therter, Pascal Dihé <pascal@cismet.de>
 * @version  $Revision$, $Date$
 */
public class H2GeoJsonJoiner {

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger LOG = Logger.getLogger(H2GeoJsonJoiner.class);
    private static final String SPATIAL_INIT = "CALL SPATIAL_INIT();";
    private static final String CREATE_SPATIAL_INIT_ALIAS =
        "CREATE ALIAS IF NOT EXISTS SPATIAL_INIT FOR  \"org.h2gis.h2spatialext.CreateSpatialExtension.initSpatialExtension\";";
    private static final String TRANSFORM =
        "update %TABLE_NAME% set the_geom = st_transform(st_setsrid(the_geom, %MERGE_SRID%), %EXPORT_SRID%)";
    private static final String QUERY =
        "select export.*, %EXPORT_PARAMETERS% from %EXPORT_TABLE% export left join %MERGE_TABLE% merge on (export.the_geom && merge.the_geom and st_intersects(export.the_geom, merge.the_geom))";
    private static final String CREATE_SPATIAL_INDEX =
        "CREATE SPATIAL INDEX %INDEX_NAME% ON %TABLE_NAME% (%COLUMN_NAME%);";

    // Pfade und Tabellennamen eventuell aendern
    private static final String DB_NAME = "tmpDatabase";
    private static final String TABLE_NAME = "table";

    //~ Instance fields --------------------------------------------------------

    private int tableCount = 0;
    private final ConnectionWrapper exportConnection;
    private final String dbPath;
    private final File exportDataShape;
    private final File mergeGeoJson;
    private final int exportCrs;
    private final int mergeCrs;
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
        this.dbPath = createTempDirectory().getAbsolutePath();
        this.exportConnection = getDBConnection(dbPath + "/" + DB_NAME);
        this.exportDataShape = getUnzippedGeojsonFileFromByteArray(exportData);
        this.mergeGeoJson = getUnzippedGeojsonFileFromByteArray(mergeData);
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

        LOG.info("mering Shape file '" + exportDataShape.getName() + "' (EPSG:"
                    + this.exportCrs + ") with " + this.mergeParameters.size()
                    + "' properties of GeoJSON file '" + mergeGeoJson.getName() + "' (EPSG:"
                    + this.mergeCrs);

        this.initDatabase(exportConnection);
        final String exportDataTable = importShpFileToDb(
                exportConnection,
                exportDataShape.getAbsolutePath());

        final String mergeDataTable = importGeoJsonFileToDb(
                exportConnection,
                mergeGeoJson.getAbsolutePath(),
                mergeCrs,
                exportCrs);
        final StatementWrapper st = createStatement(exportConnection);

        String query = QUERY.replace("%EXPORT_TABLE%", exportDataTable);
        query = query.replace("%MERGE_TABLE%", mergeDataTable);

        final StringBuilder mergeParameterBuilder = new StringBuilder();
        final Iterator<Parameter> mergeParameterIterator = this.mergeParameters.iterator();
        while (mergeParameterIterator.hasNext()) {
            final Parameter parameter = mergeParameterIterator.next();

            mergeParameterBuilder.append('\'').append(parameter.getParameterName()).append('\'');
            mergeParameterBuilder.append(" AS \'").append(parameter.getParameterName()).append('\'');
            if (mergeParameterIterator.hasNext()) {
                mergeParameterBuilder.append(',');
            }
        }

        query = query.replace("%EXPORT_PARAMETERS%", mergeParameterBuilder.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug(query);
        }

        try(final ResultSet rs = st.executeQuery(query)) {
            return rs;
        }
    }

    /**
     * DOCUMENT ME!
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
     * Imports the given shape file.
     *
     * @param   connectionWrapper  con DOCUMENT ME!
     * @param   geojsonFile        shpFile DOCUMENT ME!
     * @param   mergeSrid          DOCUMENT ME!
     * @param   exportSrid         DOCUMENT ME!
     *
     * @return  the name of the table, that contains the given shape file
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private String importGeoJsonFileToDb(
            final ConnectionWrapper connectionWrapper,
            final String geojsonFile,
            final int mergeSrid,
            final int exportSrid) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("importing geojson File for merging: '" + geojsonFile + "'.");
        }

        final String table = TABLE_NAME + "_" + (++tableCount);
        try(final StatementWrapper statementWrapper = createStatement(connectionWrapper)) {
            statementWrapper.execute("CALL GeoJsonRead('" + geojsonFile + "', '" + table + "');");

            if (mergeSrid != exportSrid) {
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
     * DOCUMENT ME!
     *
     * @param   connectionWrapper  DOCUMENT ME!
     * @param   shpFile            DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private String importShpFileToDb(
            final ConnectionWrapper connectionWrapper,
            final String shpFile) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("importing Shape File for merging: '" + shpFile + "'.");
        }

        final String table = TABLE_NAME + "_" + (++tableCount);
        try(final StatementWrapper statementWrapper = createStatement(connectionWrapper)) {
            statementWrapper.execute("CALL FILE_TABLE('" + shpFile + "', '" + table + "');");
        }

        createSpatialIndex("the_geom", table);

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
     * @param   array  DOCUMENT ME!
     *
     * @return  the unzipped shape file
     *
     * @throws  IOException  DOCUMENT ME!
     */
    private File getUnzippedGeojsonFileFromByteArray(final byte[] array) throws IOException {
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

            final File[] unzippedShpFiles = directory.listFiles();

            if ((unzippedShpFiles != null) && (unzippedShpFiles.length > 0)) {
                return unzippedShpFiles[0];
            } else {
                return directory;
            }
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
     * @param   conn  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    private void initDatabase(final ConnectionWrapper conn) throws SQLException {
        try(final ResultSet rs = conn.getMetaData().getTables(null, null, "GEOMETRY_COLUMNS", null)) {
            if (!rs.next()) {
                try(final StatementWrapper st = createStatement(conn)) {
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
}
