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
package de.cismet.cids.custom.udm2020di.indeximport.boris;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class BorisImport extends OracleImport {

    //~ Instance fields --------------------------------------------------------

    String getSitesStatementTpl;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BorisImport object.
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public BorisImport() throws Exception {
        this(BorisImport.class.getResourceAsStream("boris.properties"));
    }

    /**
     * Creates a new BorisImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public BorisImport(final InputStream propertiesFile) throws Exception {
        super(propertiesFile);
        getSitesStatementTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/templates/select-boris-sites.sql"),
                "UTF-8");
    }

    /**
     * Creates a new BorisImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public BorisImport(final Path propertiesFile) throws Exception {
        this(Files.newInputStream(null, StandardOpenOption.READ));
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @throws  IOException   DOCUMENT ME!
     * @throws  SQLException  DOCUMENT ME!
     */
    public void doBootstrap() throws IOException, SQLException {
        final String truncateBorisTablesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/bootstrap/truncate-boris-tables.sql"),
                "UTF-8");
        this.executeBatchStatement(targetConnection, truncateBorisTablesTpl);

        final String insertBorisTaggroups = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/bootstrap/insert-boris-taggroups.sql"),
                "UTF-8");
        this.targetConnection.createStatement().execute(insertBorisTaggroups);
    }

    /**
     * DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    public void doImport() throws SQLException, IOException {
        final Statement getSitesStatement = this.sourceConnection.createStatement();
        getSitesStatement.closeOnCompletion();
        final long startTime = System.currentTimeMillis();
        log.info("fetching BORIS sites .....");
        final ResultSet sitesResultSet = getSitesStatement.executeQuery(getSitesStatementTpl);
        log.info("BORIS sites fetched in " + (System.currentTimeMillis() - startTime) + "ms");
        int i = 0;

        while (sitesResultSet.next()) {
            String tmpStr = sitesResultSet.getNString("STANDORT_PK");
            final String siteSrcPk = tmpStr;
            if (log.isDebugEnabled()) {
                log.debug("processing boris site #" + (++i) + ": " + siteSrcPk);
            }

            tmpStr = sitesResultSet.getNString("STANDORTBEZEICHNUNG");
            String siteName = ((tmpStr != null) && !tmpStr.isEmpty()) ? tmpStr
                                                                      : sitesResultSet.getNString("STANDORTNUMMER");
            siteName = ((siteName != null) && !siteName.isEmpty()) ? siteName
                                                                   : sitesResultSet.getNString("STANDORT_PK");

            // key
            final String siteKey = "BORIS." + sitesResultSet.getNString("STANDORT_PK");

            // description
            final String siteDescription = new StringBuilder().append("STANDORTBEZEICHNUNG: ")
                        .append(sitesResultSet.getNString("STANDORTBEZEICHNUNG"))
                        .append('\n')
                        .append("STANDORTNUMMER: ")
                        .append(sitesResultSet.getNString("STANDORTNUMMER"))
                        .append('\n')
                        .append("STANDORT PK in BORIS: ")
                        .append(sitesResultSet.getNString("STANDORT_PK"))
                        .append('\n')
                        .toString();

            // LITERATUR TAG for Catalogue
            tmpStr = sitesResultSet.getNString("LITERATUR");
            final String siteLiteraturTagKey = Integer.toHexString(tmpStr.hashCode());
            this.insertUniqueTag(siteLiteraturTagKey, tmpStr, tmpStr, "BORIS.LITERATUR");

            // INSITUT TAG for Catalogue
            tmpStr = sitesResultSet.getNString("INSTITUT");
            final String siteInstitutTagKey = Integer.toHexString(tmpStr.hashCode());
            this.insertUniqueTag(siteInstitutTagKey, tmpStr, tmpStr, "BORIS.INSTITUT");

            // GEOM
            final float siteHochwert = sitesResultSet.getFloat("HOCHWERT");     // X
            final float siteRechtswert = sitesResultSet.getFloat("RECHTSWERT"); // Y
            final long siteGeomId = this.insertGeomPoint(siteHochwert, siteRechtswert, 31287, 4326);
            // SRC JSON CONTENT
            final String siteSrcContent = this.xmlToJson(sitesResultSet.getClob("STANDORT_XML"));
        }

        // PreparedStatement s = targetConnection.prepareStatement(getSitesStatementTpl,
        // Statement.RETURN_GENERATED_KEYS);
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  DOCUMENT ME!
     */
    public static void main(final String[] args) {
        final BorisImport borisImport;
        try {
            if (args.length > 0) {
                log.info("loading BORIS properties from: " + args[0]);
                borisImport = new BorisImport(FileSystems.getDefault().getPath(args[0]));
            } else {
                borisImport = new BorisImport();
            }

            borisImport.doBootstrap();
            // borisImport.doImport();
        } catch (Exception ex) {
            log.error("could not create BORIS import instance: " + ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
