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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import oracle.jdbc.OraclePreparedStatement;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class BorisImport extends OracleImport {

    //~ Instance fields --------------------------------------------------------

    protected final String getSitesStatementTpl;
    protected final PreparedStatement getSampleValues;
    protected final PreparedStatement insertSite;
    protected final OraclePreparedStatement insertSampleValues;
    protected final PreparedStatement insertSiteValuesRel;
    protected final PreparedStatement insertSiteTagsRel;
    protected final PreparedStatement getTags;
    protected final PreparedStatement updateSiteJson;
    protected final HashMap<String, String[]> parameterMappings = new HashMap<String, String[]>();

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
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/select-boris-sites.prs.sql"),
                "UTF-8");

        final String getBorisAggregatedSampleValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/select-boris-aggregated-sample-values.prs.sql"),
                "UTF-8");

        getSampleValues = this.sourceConnection.prepareStatement(getBorisAggregatedSampleValuesTpl);

        final String insertBorisSiteTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/insert-boris-site.prs.sql"),
                "UTF-8");
        insertSite = this.targetConnection.prepareStatement(insertBorisSiteTpl, new String[] { "ID" });

        final String insertBorisSampleValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/insert-boris-aggregated-sample-values.prs.sql"),
                "UTF-8");
        insertSampleValues = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertBorisSampleValuesTpl,
                new String[] { "ID" });
//        insertSampleValues.setExecuteBatch(200);
//        log.debug ("insertBorisSampleValues Statement Execute Batch Value " +
//                   insertSampleValues.getExecuteBatch());

        final String insertBorisSiteSampleValuesRelTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/insert-boris-site-sample-values-relation.prs.sql"),
                "UTF-8");
        insertSiteValuesRel = this.targetConnection.prepareStatement(
                insertBorisSiteSampleValuesRelTpl,
                new String[] { "ID" });

        final String insertBorisSiteTagsRelTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/insert-boris-site-tags-relation.prs.sql"),
                "UTF-8");
        insertSiteTagsRel = this.targetConnection.prepareStatement(insertBorisSiteTagsRelTpl);

        final String getTagsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/boris/get-boris-tags.prs.sql"),
                "UTF-8");
        getTags = this.targetConnection.prepareStatement(getTagsTpl);

        final String updateSiteJsonTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/update-boris-site-json.prs.sql"),
                "UTF-8");
        updateSiteJson = this.targetConnection.prepareStatement(getTagsTpl);

        // load and cache mappings
        final String selectBorisParameterMappingsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/select-boris-parameter-mappings.sql"),
                "UTF-8");

        final Statement selectBorisParameterMappings = this.targetConnection.createStatement();
        final ResultSet mappingsResultSet = selectBorisParameterMappings.executeQuery(selectBorisParameterMappingsTpl);
        while (mappingsResultSet.next()) {
            this.parameterMappings.put(
                mappingsResultSet.getNString(1),
                new String[] {
                    mappingsResultSet.getNString(2),
                    mappingsResultSet.getNString(3),
                    mappingsResultSet.getNString(4)
                });
        }
        if (log.isDebugEnabled()) {
            log.debug(this.parameterMappings.size() + " parameter mappings cached");
        }
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
        if (log.isDebugEnabled()) {
            log.debug("Cleaning and Bootstrapping BORIS Tables");
        }

        final String truncateBorisTablesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/truncate-boris-tables.sql"),
                "UTF-8");
        this.executeBatchStatement(targetConnection, truncateBorisTablesTpl);

        final String insertBorisTaggroupsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/boris/insert-boris-taggroups.sql"),
                "UTF-8");

        final Statement insertBorisTaggroups = this.targetConnection.createStatement();
        insertBorisTaggroups.execute(insertBorisTaggroupsTpl);

        this.targetConnection.commit();
        insertBorisTaggroups.close();

        log.info("BORIS Tables successfully bootstrapped");
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    public int doImport() throws SQLException, IOException {
        final Statement getSitesStatement = this.sourceConnection.createStatement();
        // getSitesStatement.closeOnCompletion();
        long startTime = System.currentTimeMillis();
        log.info("fetching BORIS sites from Source Connection " + this.sourceConnection.getSchema());
        final ResultSet sitesResultSet = getSitesStatement.executeQuery(getSitesStatementTpl);
        if (log.isDebugEnabled()) {
            log.debug("BORIS sites fetched in " + ((System.currentTimeMillis() - startTime) / 1000) + "s");
        }
        int i = 0;

        while (sitesResultSet.next()) {
            try {
                startTime = System.currentTimeMillis();

                String tmpStr = sitesResultSet.getNString("STANDORT_PK");
                final String siteSrcPk = tmpStr;
                if (log.isDebugEnabled()) {
                    log.debug("processing BORIS Site #" + (++i) + ": " + siteSrcPk);
                }

                // key
                final String siteKey = "BORIS." + tmpStr;

                tmpStr = sitesResultSet.getNString("STANDORTBEZEICHNUNG");
                String siteName = ((tmpStr != null) && !tmpStr.isEmpty()) ? tmpStr
                                                                          : sitesResultSet.getNString("STANDORTNUMMER");
                siteName = ((siteName != null) && !siteName.isEmpty()) ? siteName
                                                                       : sitesResultSet.getNString("STANDORT_PK");
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
                // -> INSERT LITERATUR TAG
                this.insertUniqueTag(siteLiteraturTagKey, tmpStr, tmpStr, "BORIS.LITERATUR");

                // INSITUT TAG for Catalogue
                tmpStr = sitesResultSet.getNString("INSTITUT");
                final String siteInstitutTagKey = Integer.toHexString(tmpStr.hashCode());
                // -> INSERT INSTITUT TAG
                this.insertUniqueTag(siteInstitutTagKey, tmpStr, tmpStr, "BORIS.INSTITUT");

                // GEOM
                final float siteHochwert = sitesResultSet.getFloat("HOCHWERT");     // X
                final float siteRechtswert = sitesResultSet.getFloat("RECHTSWERT"); // Y
                // -> INSERT GEOM and GET ID!
                final long siteGeomId = this.insertGeomPoint(siteHochwert, siteRechtswert, 31287, 4326);
                if (siteGeomId == -1) {
                    continue;
                }

                // SRC JSON CONTENT
                // final String siteSrcContent = this.xmlClobToJsonString(sitesResultSet.getClob("STANDORT_XML"));

                // -> INSERT SITE
                final long borisSiteId = insertSite(
                        siteKey,
                        siteName,
                        siteDescription,
                        siteLiteraturTagKey,
                        siteInstitutTagKey,
                        siteGeomId,
                        siteSrcPk,
                        null);
                if (borisSiteId == -1) {
                    continue;
                }

                // PARSE AND UPDATE JSON
                final ObjectNode jsonObject = (ObjectNode)xmlMapper.readTree(sitesResultSet.getClob("STANDORT_XML")
                                .getCharacterStream());

                // -> SAMPLE VALUES AND TAGS
                final Collection<Long> sampeValueIds = getAndInsertSampleValues(siteSrcPk, jsonObject);
                if (!sampeValueIds.isEmpty()) {
                    this.insertSiteValuesRelation(borisSiteId, sampeValueIds);
                    this.insertBorisSiteTagsRelation(borisSiteId);
                }

                this.updateSrcJson(borisSiteId, jsonObject);

                // save the site
                this.targetConnection.commit();

                if (log.isDebugEnabled()) {
                    log.info("BORIS Site #" + (i) + ": " + siteSrcPk
                                + " with " + sampeValueIds.size()
                                + " aggregated sample values processed and imported in "
                                + (System.currentTimeMillis() - startTime) + "ms");
                }
            } catch (Throwable t) {
                log.error("rolling back BORIS Site #" + (i) + ": "
                            + " due to error: " + t.getMessage(), t);
                try {
                    this.targetConnection.rollback();
                } catch (SQLException sx) {
                    log.error("could not rollback target connection", sx);
                }

                i--;
            }

            // test mode
            break;
        }
        if (log.isDebugEnabled()) {
            // clean up
            log.debug("closing connections ....");
        }
        this.getSampleValues.close();

        this.insertGenericGeom.close();
        this.insertUniqueTag.close();

        this.insertSite.close();
        this.insertSampleValues.close();
        this.insertSiteValuesRel.close();
        this.insertSiteTagsRel.close();
        this.updateSiteJson.close();
        this.getTags.close();

        this.sourceConnection.close();
        this.targetConnection.close();

        return i;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   borisSiteId  DOCUMENT ME!
     * @param   jsonNode     DOCUMENT ME!
     *
     * @throws  SQLException             DOCUMENT ME!
     * @throws  JsonProcessingException  DOCUMENT ME!
     */
    protected void updateSrcJson(final long borisSiteId, final ObjectNode jsonNode) throws SQLException,
        JsonProcessingException {
        getTags.setLong(1, borisSiteId);
        final ResultSet getTagsResult = getTags.executeQuery();

        // put the resultset in a containing structure
        jsonNode.putPOJO("TAGS", getTagsResult);

        try {
            final String jsonString = this.jsonMapper.writeValueAsString(jsonNode);
            //updateSiteJson.setClob(1, new StringReader(jsonString));
            //updateSiteJson.setString(1, jsonString);
            updateSiteJson.setCharacterStream(1, new StringReader(jsonString), jsonString.length());
        } catch (JsonProcessingException jpe) {
            log.error("could not deserialize JSON: " + jpe.getMessage(), jpe);
            getTagsResult.close();
            throw jpe;
        }

        updateSiteJson.execute();
        getTagsResult.close();
        if (log.isDebugEnabled()) {
            log.debug("JSON Content of BORIS Site " + borisSiteId + " successfully updated");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   borisSiteId  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected void insertBorisSiteTagsRelation(final long borisSiteId) throws SQLException {
        this.insertSiteTagsRel.setLong(1, borisSiteId);
        this.insertSiteTagsRel.setLong(2, borisSiteId);
        this.insertSiteTagsRel.setLong(3, borisSiteId);
        this.insertSiteTagsRel.setLong(4, borisSiteId);

        this.insertSiteTagsRel.executeUpdate();
        if (log.isDebugEnabled()) {
            log.debug("SiteTagsRelation created");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   borisSiteId    DOCUMENT ME!
     * @param   sampeValueIds  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected void insertSiteValuesRelation(final long borisSiteId, final Collection<Long> sampeValueIds)
            throws SQLException {
        for (final long sampeValueId : sampeValueIds) {
            this.insertSiteValuesRel.setLong(1, borisSiteId);
            this.insertSiteValuesRel.setLong(2, sampeValueId);
            this.insertSiteValuesRel.addBatch();
        }

        this.insertSiteValuesRel.executeBatch();
        if (log.isDebugEnabled()) {
            // this.insertSiteValuesRel.close();
            log.debug(sampeValueIds.size() + " Site-Values-Relations created");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   siteSrcPk   DOCUMENT ME!
     * @param   jsonObject  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     * @throws  IOException   DOCUMENT ME!
     */
    protected Collection<Long> getAndInsertSampleValues(final String siteSrcPk, final ObjectNode jsonObject)
            throws SQLException, IOException {
        final Collection<Long> sampeValueIds = new HashSet<Long>();
        int i = 0;
        int added = 0;

        final ArrayNode jsonValues = jsonObject.putArray("VALUES");

        // <- GET AGGREGATED SAMPLE VALUES
        this.getSampleValues.setString(1, siteSrcPk);
        final ResultSet sampleValuesResultSet = this.getSampleValues.executeQuery();
        // build the batch insert statements
        while (sampleValuesResultSet.next()) {
            final String PARAMETER_PK = sampleValuesResultSet.getString("PARAMETER_PK");
            i++;
            if (this.parameterMappings.containsKey(PARAMETER_PK)) {
                final ObjectNode jsonValue = this.jsonMapper.createObjectNode();
                jsonValues.add(jsonValue);

                final String[] mappedParameters = this.parameterMappings.get(PARAMETER_PK);
                // NAME
                // log.debug(mappedParameters[0]);
                this.insertSampleValues.setStringAtName("NAME", mappedParameters[0]);
                jsonValue.put("NAME", mappedParameters[0]);
                // this.insertSampleValues.setString(1, mappedParameters[0]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + mappedParameters[1]);
// }
                // POLLUTANT
                this.insertSampleValues.setStringAtName("POLLUTANT", mappedParameters[1]);
                jsonValue.put("POLLUTANT", mappedParameters[1]);
                // this.insertSampleValues.setString(2, mappedParameters[1]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + mappedParameters[2]);
// }
                // POLLUTANT_GROUP
                this.insertSampleValues.setStringAtName("POLLUTANT_GROUP", mappedParameters[2]);
                jsonValue.put("POLLUTANT", mappedParameters[2]);
                // this.insertSampleValues.setString(3, mappedParameters[2]);
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getDate("MIN_DATE"));
// }
                final Date minDate = sampleValuesResultSet.getDate("MIN_DATE");
                this.insertSampleValues.setDateAtName("MIN_DATE", minDate);
                jsonValue.put("MIN_DATE", DATE_FORMAT.format(minDate));
                // this.insertSampleValues.setDate(4, sampleValuesResultSet.getDate("MIN_DATE")); if
                // (log.isDebugEnabled()) { log.debug("["+added+"] " + sampleValuesResultSet.getDate("MAX_DATE")); }

                final Date maxDate = sampleValuesResultSet.getDate("MAX_DATE");
                this.insertSampleValues.setDateAtName("MAX_DATE", maxDate);
                jsonValue.put("MAX_DATE", DATE_FORMAT.format(maxDate));

                // this.insertSampleValues.setDate(5, sampleValuesResultSet.getDate("MAX_DATE"));
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getFloat("MIN_VALUE"));
// }
                final float minValue = sampleValuesResultSet.getFloat("MIN_VALUE");
                this.insertSampleValues.setFloatAtName("MIN_VALUE", minValue);
                jsonValue.put("MIN_VALUE", minValue);
                // this.insertSampleValues.setFloat(6, sampleValuesResultSet.getFloat("MIN_VALUE"));
// if (log.isDebugEnabled()) {
// log.debug("["+added+"] " + sampleValuesResultSet.getFloat("MAX_VALUE"));
// }
                final float maxValue = sampleValuesResultSet.getFloat("MAX_VALUE");
                this.insertSampleValues.setFloatAtName("MAX_VALUE", maxValue);
                jsonValue.put("MAX_VALUE", maxValue);
                // this.insertSampleValues.setFloat(7, sampleValuesResultSet.getFloat("MAX_VALUE"));

                final String srcContentJson = this.xmlClobToJsonString(sampleValuesResultSet.getClob("MESSWERTE_XML"));
                // SRC_CONTENT
                // log.debug(srcContentJson);
                this.insertSampleValues.setStringAtName("SRC_CONTENT", srcContentJson);
                this.insertSampleValues.setString(8, srcContentJson);

                // FIXME: Execute Batch does not work with large updates!!!!!
                // this.insertSampleValues.addBatch();

                this.insertSampleValues.executeUpdate();
                final ResultSet generatedKeys = this.insertSampleValues.getGeneratedKeys();
                if ((null != generatedKeys)) {
                    while (generatedKeys.next()) {
                        sampeValueIds.add(generatedKeys.getLong(1));
                    }
                    generatedKeys.close();
                    added++;
                } else {
                    log.error("could not fetch generated key for inserted samples values for BORIS SITE " + siteSrcPk);
                }
            }
        }

        sampleValuesResultSet.close();

        if (added > 0) {
// FIXME: Execute Batch does not work with large updates!!!!!
//            if (log.isDebugEnabled()) {
//                log.debug("adding " + added + " of " + i + " sample values for BORIS Site " + siteSrcPk);
//            }
//            this.insertSampleValues.executeBatch();

//            final ResultSet generatedKeys = this.insertSampleValues.getGeneratedKeys();
//            if ((null != generatedKeys)) {
//                while (generatedKeys.next()) {
//                    sampeValueIds.add(generatedKeys.getLong(1));
//                }
//                generatedKeys.close();
//                if (log.isDebugEnabled()) {
//                    log.debug(added + " of " + i + " sample values added for BORIS Site " + siteSrcPk
//                    + ", " + sampeValueIds.size() + " IDs generated");
//                }
//            } else {
//                log.error("could not fetch generated key for inserted samples values for BORIS SITE " + siteSrcPk);
//            }

            if (log.isDebugEnabled()) {
                log.debug(added + " of " + i + " sample values added for BORIS Site " + siteSrcPk
                            + ", " + sampeValueIds.size() + " IDs generated");
            }
        } else {
            log.warn("no supported sample values found in " + i + " available sample values for BORIS SITE "
                        + siteSrcPk);
        }

        return sampeValueIds;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   siteKey              DOCUMENT ME!
     * @param   siteName             DOCUMENT ME!
     * @param   siteDescription      DOCUMENT ME!
     * @param   siteLiteraturTagKey  DOCUMENT ME!
     * @param   siteInstitutTagKey   DOCUMENT ME!
     * @param   siteGeomId           DOCUMENT ME!
     * @param   siteSrcPk            DOCUMENT ME!
     * @param   siteSrcContent       DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected long insertSite(final String siteKey,
            final String siteName,
            final String siteDescription,
            final String siteLiteraturTagKey,
            final String siteInstitutTagKey,
            final long siteGeomId,
            final String siteSrcPk,
            final String siteSrcContent) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("inserting BORIS Site " + siteKey + ": '" + siteName + "'");
        }
        final long startTime = System.currentTimeMillis();

        this.insertSite.setString(1, siteKey);
        this.insertSite.setString(2, siteName);
        this.insertSite.setString(3, siteDescription);
        this.insertSite.setLong(4, siteGeomId);
        this.insertSite.setString(5, siteLiteraturTagKey);
        this.insertSite.setString(6, siteInstitutTagKey);
        this.insertSite.setString(7, siteSrcPk);
        this.insertSite.setString(8, siteSrcContent);

        this.insertSite.executeUpdate();
        final ResultSet generatedSiteKeysRs = this.insertSite.getGeneratedKeys();
        long generatedKey = -1;

        if ((null != generatedSiteKeysRs) && generatedSiteKeysRs.next()) {
            generatedKey = generatedSiteKeysRs.getLong(1);
        } else {
            log.error("could not fetch generated key for inserted BORIS SITE!");
        }
        if (log.isDebugEnabled()) {
            // this.insertSite.close();
            log.debug("BORIS site " + siteKey + " inserted in " + (System.currentTimeMillis() - startTime)
                        + "ms, new ID is "
                        + generatedKey);
        }
        return generatedKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  DOCUMENT ME!
     */
    public static void main(final String[] args) {
        BorisImport borisImport = null;
        try {
            if (args.length > 0) {
                if (log.isDebugEnabled()) {
                    log.debug("loading BORIS properties from: " + args[0]);
                }
                borisImport = new BorisImport(FileSystems.getDefault().getPath(args[0]));
            } else {
                borisImport = new BorisImport();
            }

            final long startTime = System.currentTimeMillis();
            BorisImport.log.info("Starting BORIS Import ......");

            borisImport.doBootstrap();
            final int sites = borisImport.doImport();

            BorisImport.log.info(sites + " BORIS Sites successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
        } catch (Exception ex) {
            log.error("could not create BORIS import instance: " + ex.getMessage(), ex);
        } finally {
            try {
                if (borisImport != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("closing source connection");
                    }
                    borisImport.sourceConnection.close();
                }
            } catch (SQLException ex) {
                log.error("could not close source connection", ex);
                System.exit(1);
            }

            try {
                if (borisImport != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("closing target connection");
                    }
                    borisImport.targetConnection.close();
                }
            } catch (SQLException ex) {
                log.error("could not close target connection", ex);
                System.exit(1);
            }
        }
    }
}
