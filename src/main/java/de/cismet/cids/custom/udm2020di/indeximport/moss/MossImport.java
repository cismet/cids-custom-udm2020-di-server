/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport.moss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import oracle.jdbc.OraclePreparedStatement;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import de.cismet.cids.custom.udm2020di.dataexport.XlsHelper;
import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.AggregationValues;
import de.cismet.cids.custom.udm2020di.types.Parameter;
import de.cismet.cids.custom.udm2020di.types.ParameterMapping;
import de.cismet.cids.custom.udm2020di.types.ParameterMappings;
import de.cismet.cids.custom.udm2020di.types.moss.Moss;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class MossImport extends OracleImport {

    //~ Static fields/initializers ---------------------------------------------

    public static final String DEFAULT_IMPORTFILE = "konvert_join_95_10_final.xls";

    //~ Instance fields --------------------------------------------------------

    protected final OraclePreparedStatement insertStationStmnt;
    protected final OraclePreparedStatement deleteStationStmnt;
    protected final OraclePreparedStatement insertSampleValuesStmnt;
    protected final OraclePreparedStatement updateStationJsonStnmt;
    protected final OraclePreparedStatement getTagsStmnt;
    protected final ParameterMappings parameterMappings = new ParameterMappings();
    protected final Map<String, Moss> mossStations = new HashMap<String, Moss>();

    protected final Map<String, Long> mossIdMap = new HashMap<String, Long>();
    protected final Map<String, AggregationValues> aggregationValuesMap = new HashMap<String, AggregationValues>();

    protected final Sheet mossSheet;
    protected final Map<String, java.util.Date> dateMap = new HashMap<String, java.util.Date>();

    protected final XlsHelper xlsHelper = new XlsHelper();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new MossImport object.
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public MossImport() throws Exception {
        this(MossImport.class.getResourceAsStream("moss.properties"),
            MossImport.class.getResourceAsStream(DEFAULT_IMPORTFILE));
    }

    /**
     * Creates a new MossImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     * @param   importFile      DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public MossImport(final InputStream propertiesFile, final InputStream importFile) throws Exception {
        super(propertiesFile);
        this.log = Logger.getLogger(MossImport.class);

        dateMap.put("konv_10", new GregorianCalendar(2010, 0, 01).getTime());
        dateMap.put("00", new GregorianCalendar(2000, 0, 01).getTime());
        dateMap.put("95", new GregorianCalendar(1995, 0, 01).getTime());
        dateMap.put("05", new GregorianCalendar(2005, 0, 01).getTime());

        final String insertMossStationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/moss/insert-moss-station.prs.sql"),
                "UTF-8");
        insertStationStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertMossStationTpl,
                new String[] { "ID" });

        final String deleteMossStationTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/moss/delete-moss-station.prs.sql"),
                "UTF-8");
        deleteStationStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                deleteMossStationTpl);

        final String insertMossSampleValuesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/moss/insert-moss-sample-values.prs.sql"),
                "UTF-8");

        insertSampleValuesStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                insertMossSampleValuesTpl,
                new String[] { "ID" });

        final String updateStationJsonTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/moss/update-moss-station-json.prs.sql"),
                "UTF-8");
        updateStationJsonStnmt = (OraclePreparedStatement)this.targetConnection.prepareStatement(
                updateStationJsonTpl);

        final String getTagsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/moss/get-moss-tags.prs.sql"),
                "UTF-8");
        getTagsStmnt = (OraclePreparedStatement)this.targetConnection.prepareStatement(getTagsTpl);

        // load and cache mappings
        final String selectMossParameterMappingsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/moss/select-moss-parameter-mappings.sql"),
                "UTF-8");

        final Statement selectMossParameterMappings = this.targetConnection.createStatement();
        final ResultSet mappingsResultSet = selectMossParameterMappings.executeQuery(selectMossParameterMappingsTpl);

        final ParameterMapping[] parameterMappingArray = this.deserializeResultSet(
                mappingsResultSet,
                ParameterMapping[].class);
        for (final ParameterMapping parameterMapping : parameterMappingArray) {
            this.parameterMappings.put(parameterMapping.getParameterPk(),
                parameterMapping);
        }

        mappingsResultSet.close();
        selectMossParameterMappings.close();

        if (log.isDebugEnabled()) {
            log.debug(this.parameterMappings.size() + " parameter mappings cached");
        }

        final Workbook workbook = WorkbookFactory.create(importFile);
        if (log.isDebugEnabled()) {
            log.debug("reading XLS sheet '" + workbook.getSheetName(0)
                        + "' from workbook with " + workbook.getNumberOfSheets() + " sheets");
        }
        this.mossSheet = workbook.getSheetAt(0);
    }

    /**
     * Creates a new MossImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     * @param   importFile      DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public MossImport(final Path propertiesFile, final Path importFile) throws Exception {
        this(Files.newInputStream(propertiesFile, StandardOpenOption.READ),
            Files.newInputStream(importFile, StandardOpenOption.READ));
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   stationSrcPk  DOCUMENT ME!
     * @param   stationRow    DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException  DOCUMENT ME!
     */
    protected long getAndInsertMossStation(final String stationSrcPk,
            final Row stationRow) throws SQLException {
        final long startTime = System.currentTimeMillis();
        long mossStationId = -1;
        String tmpStr;
        final String stationKey;
        final String stationName;
        final String stationDescription;
        final String stationTypeTagKey;
        final long stationGeomId;

        final Moss mossStation = new Moss();
        mossStation.setSampleId(stationSrcPk);

        // key
        stationKey = "MOSS." + stationSrcPk;

        // name
        tmpStr = xlsHelper.getCellValue("Moos_Text", stationRow).toString();
        stationName = stationSrcPk + " (" + tmpStr + ")";
        mossStation.setType(tmpStr);

        // type
        stationTypeTagKey = Integer.toHexString(tmpStr.hashCode());
        // -> INSERT MOSS_TYPE TAG
        this.insertUniqueTag(
            stationTypeTagKey,
            tmpStr,
            tmpStr,
            "MOSS.MOSS_TYPE");

        // lab_no
        tmpStr = xlsHelper.getCellValue("Labornr", stationRow).toString();
        mossStation.setLabNo(tmpStr);

        // description
        stationDescription = new StringBuilder().append("PROBENNUMMER: ").append(stationName).append('\n')
                    .append("MOOS TYP: ")
                    .append(mossStation.getType())
                    .append('\n')
                    .append("LABORNUMMER: ")
                    .append(mossStation.getLabNo())
                    .toString();

        // GEOM
        final float xCoordinate = ((Double)xlsHelper.getCellValue("geogrLaeng", stationRow)).floatValue(); // X
        final float yCoordinate = ((Double)xlsHelper.getCellValue("geogrBreit", stationRow)).floatValue(); // Y
        // -> INSERT GEOM and GET ID!
        stationGeomId = this.insertGeomPoint(xCoordinate, yCoordinate, 4326, 4326);
        if (log.isDebugEnabled()) {
            log.debug("inserting new moss station '" + stationName + "'");
        }
        this.insertStationStmnt.setStringAtName("KEY", stationKey);
        this.insertStationStmnt.setStringAtName("NAME", stationName);
        this.insertStationStmnt.setStringAtName("DESCRIPTION", stationDescription);
        this.insertStationStmnt.setLongAtName("GEOMETRY", stationGeomId);
        this.insertStationStmnt.setStringAtName("MOSS_TYPE", stationTypeTagKey);
        this.insertStationStmnt.setStringAtName("SRC_SAMPLE_ID", stationSrcPk);

        this.insertStationStmnt.executeUpdate();
        final ResultSet generatedStationKeysRs = this.insertStationStmnt.getGeneratedKeys();

        if ((null != generatedStationKeysRs) && generatedStationKeysRs.next()) {
            mossStationId = generatedStationKeysRs.getLong(1);
            generatedStationKeysRs.close();
        } else {
            log.error("could not fetch generated key for inserted Moss Station!");
        }

        if (log.isDebugEnabled()) {
            log.debug("Moss station " + stationKey + " inserted in "
                        + (System.currentTimeMillis() - startTime)
                        + "ms, new ID is "
                        + mossStationId);
        }

        this.mossIdMap.put(stationSrcPk, mossStationId);
        mossStation.setId(mossStationId);
        this.mossStations.put(stationSrcPk, mossStation);
        this.aggregationValuesMap.put(stationSrcPk, new AggregationValues());

        return mossStationId;
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
        final int rows = this.mossSheet.getLastRowNum() + 1;
        log.info("fetching Moss stations from XLS Sheet " + this.mossSheet.getSheetName()
                    + " with " + rows + " rows.");

        final Iterator<Row> rowIterator = this.mossSheet.rowIterator();
        int i = 0;

        while (rowIterator.hasNext()) {
            try {
                final long startTime = System.currentTimeMillis();
                final Row row = rowIterator.next();

                if (xlsHelper.getColumnMap().isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("reading sheet header information");
                    }
                    xlsHelper.initColumnMap(row);
                    log.info("sheet header information processed, " + xlsHelper.getColumnMap().size()
                                + " columns identified");
                    continue;
                }

                final String stationSrcPk = xlsHelper.getCellValue("Proben_ID", row).toString();
                long mossStationId = -1;
                if (!this.mossIdMap.containsKey(stationSrcPk)) {
                    if (log.isDebugEnabled()) {
                        log.info("processing new Moss Station #" + (i) + ": " + stationSrcPk);
                        mossStationId = this.getAndInsertMossStation(stationSrcPk, row);
                        ++i;
                    } else {
                        mossStationId = this.mossIdMap.get(stationSrcPk);
                    }
                }

                if (mossStationId == -1) {
                    log.warn("could not find moss station for src key '" + stationSrcPk);
                    continue;
                }

                final Moss mossStation = this.mossStations.get(stationSrcPk);
                final AggregationValues aggregationValues = this.aggregationValuesMap.get(stationSrcPk);

                // -> SAMPLE VALUES AND TAGS
                final Collection<Long> sampleValuesIds = getAndInsertSampleValues(
                        mossStationId,
                        row,
                        aggregationValues,
                        mossStation.getProbenparameter());

                if (sampleValuesIds.isEmpty()) {
                    log.warn("no supported sampleValues found for station " + stationSrcPk);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(+sampleValuesIds.size() + " supported sampleValues found for station "
                                    + stationSrcPk);
                    }
                }

                // save the station
                this.targetConnection.commit();

                if (log.isDebugEnabled()) {
                    log.info("Row #" + (i) + " for Moss Station " + stationSrcPk
                                + " with " + sampleValuesIds.size()
                                + " sampleValues in row processed and imported in "
                                + (System.currentTimeMillis() - startTime) + "ms");
                }
            } catch (Throwable t) {
                log.error("rolling back Moss Station #" + (i) + ": "
                            + " due to error: " + t.getMessage(), t);
                try {
                    this.targetConnection.rollback();
                } catch (SQLException sx) {
                    log.error("could not rollback target connection", sx);
                }

                --i;
            }

            // test mode:
            // break;
        }

        i = 0;
        log.info("postprocessing " + this.mossIdMap.size() + " moss stations and adding aggregated JSON");
        for (final String stationSrcPk : this.mossIdMap.keySet()) {
            final long mossStationId = this.mossIdMap.get(stationSrcPk);
            final AggregationValues aggregationValues = this.aggregationValuesMap.get(stationSrcPk);
            final Moss mossStation = this.mossStations.get(stationSrcPk);

            if ((aggregationValues == null) || (mossStation == null)) {
                log.warn("no valid moss objects generated for moss station " + stationSrcPk);
                continue;
            }

            ++i;

            try {
                // set unique aggregation values
                mossStation.setAggregationValues(new ArrayList<AggregationValue>(aggregationValues));
                final ObjectNode jsonObject = (ObjectNode)JSON_MAPPER.valueToTree(mossStation);
                this.updateSrcJson(mossStationId, jsonObject);
            } catch (Throwable t) {
                log.error("rolling back Moss Station #" + (i) + ": "
                            + " due to error: " + t.getMessage(), t);
                try {
                    this.targetConnection.rollback();
                } catch (SQLException sx) {
                    log.error("could not rollback target connection", sx);
                }

                --i;
            }
        }

        if (log.isDebugEnabled()) {
            // clean up
            log.debug("closing connections ....");
        }
        this.insertGenericGeomStmnt.close();
        this.insertUniqueTagStmnt.close();
        this.deleteGeomStmnt.close();
        this.insertStationStmnt.close();
        this.deleteStationStmnt.close();
        this.insertSampleValuesStmnt.close();
        this.updateStationJsonStnmt.close();
        this.getTagsStmnt.close();

        this.sourceConnection.close();
        this.targetConnection.close();

        return i;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   mossStationId  DOCUMENT ME!
     * @param   jsonNode       DOCUMENT ME!
     *
     * @throws  SQLException             DOCUMENT ME!
     * @throws  JsonProcessingException  DOCUMENT ME!
     * @throws  IOException              DOCUMENT ME!
     */
    protected void updateSrcJson(
            final long mossStationId,
            final ObjectNode jsonNode) throws SQLException, JsonProcessingException, IOException {
        getTagsStmnt.setLongAtName("MOSS_ID", mossStationId);
        final ResultSet getTagsResult = getTagsStmnt.executeQuery();

        // put the resultset in a containing structure
        jsonNode.putPOJO("tags", getTagsResult);

        try {
            final Clob srcContentClob = this.targetConnection.createClob();
            final Writer clobWriter = srcContentClob.setCharacterStream(1);
            JSON_MAPPER.writeValue(clobWriter, jsonNode);

            updateStationJsonStnmt.setClob(1, srcContentClob);
            updateStationJsonStnmt.setLong(2, mossStationId);
            updateStationJsonStnmt.setLong(3, mossStationId);
            updateStationJsonStnmt.setLong(4, mossStationId);

            updateStationJsonStnmt.executeUpdate();

            clobWriter.close();
        } catch (Exception ex) {
            log.error("could not deserialize and update JSON of Moss Station "
                        + mossStationId + ": " + ex.getMessage(),
                ex);
            getTagsResult.close();
            throw ex;
        }

        getTagsResult.close();
        if (log.isDebugEnabled()) {
            log.debug("JSON Content of Moss Station " + mossStationId + " successfully updated");
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param   columnIndex  DOCUMENT ME!
     * @param   cell         DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected ParameterMapping getParameterMapping(final short columnIndex, final Cell cell) {
        final String columnName = xlsHelper.getColumnNames()[columnIndex];
        if ((columnName != null) && (columnName.indexOf('_') > 0)) {
            final String potentialPollutantKey = columnName.substring(0, columnName.indexOf('_'));
            if (this.parameterMappings.containsKey(potentialPollutantKey)) {
                return parameterMappings.get(potentialPollutantKey);
            }
        }

        return null;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   columnIndex  DOCUMENT ME!
     * @param   cell         DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected java.util.Date getSampleDate(final short columnIndex, final Cell cell) {
        final String columnName = xlsHelper.getColumnNames()[columnIndex];
        if ((columnName != null) && (columnName.indexOf('_') > 0)) {
            final String potentialDateKey = columnName.substring(columnName.indexOf('_') + 1);
            if (this.dateMap.containsKey(potentialDateKey)) {
                return dateMap.get(potentialDateKey);
            }
        }

        return null;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   mossStationId      DOCUMENT ME!
     * @param   row                stationSrcPk DOCUMENT ME!
     * @param   aggregationValues  jsonObject DOCUMENT ME!
     * @param   parameters         DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  SQLException                DOCUMENT ME!
     * @throws  IOException                 DOCUMENT ME!
     * @throws  CloneNotSupportedException  DOCUMENT ME!
     */
    protected Collection<Long> getAndInsertSampleValues(final long mossStationId,
            final Row row,
            final Collection<AggregationValue> aggregationValues,
            final Collection<Parameter> parameters) throws SQLException, IOException, CloneNotSupportedException {
        final Collection<Long> sampeValueIds = new HashSet<Long>();

        // short minColIx = row.getFirstCellNum();
        final short minColIx = 6;
        final short maxColIx = row.getLastCellNum();
        short added = 0;
        short i;
        for (i = minColIx; i < maxColIx; i++) {
            final Cell cell = row.getCell(i);
            if (cell == null) {
                if (log.isDebugEnabled()) {
                    log.warn("empty cell #" + i + " '"
                                + this.xlsHelper.getColumnNames()[i] + "' of row #" + row.getRowNum());
                }
                continue;
            }

            final ParameterMapping parameterMapping = this.getParameterMapping(i, cell);
            if (parameterMapping == null) {
                if (log.isDebugEnabled()) {
                    log.warn("unsupported value cell #" + i + " '"
                                + this.xlsHelper.getColumnNames()[i] + "' of row #" + row.getRowNum());
                }
                continue;
            }

            final java.util.Date sampleDate = this.getSampleDate(i, cell);
            if (sampleDate == null) {
                if (log.isDebugEnabled()) {
                    log.warn("unsupported date for cell #" + i + " '"
                                + this.xlsHelper.getColumnNames()[i] + "' of row #" + row.getRowNum());
                }
                continue;
            }

            final Object sampleValueObject = xlsHelper.getCellValue(cell);
            if ((sampleValueObject == null) || !Double.class.isAssignableFrom(sampleValueObject.getClass())) {
                if (log.isDebugEnabled()) {
                    log.warn("empty sample value for cell #" + i + " '"
                                + this.xlsHelper.getColumnNames()[i] + "' of row #" + row.getRowNum());
                }
                continue;
            }
            final AggregationValue aggregationValue = new AggregationValue();
            aggregationValue.setUnit(parameterMapping.getUnit());

            // NAME
            // log.debug(mappedParameters[0]);
            this.insertSampleValuesStmnt.setStringAtName("NAME", parameterMapping.getDisplayName());
            aggregationValue.setName(parameterMapping.getDisplayName());

            // STATION
            this.insertSampleValuesStmnt.setLongAtName("STATION", mossStationId);

            // POLLUTANT
            this.insertSampleValuesStmnt.setLongAtName("POLLUTANT_ID", parameterMapping.getPollutantTagId());
            aggregationValue.setPollutantKey(parameterMapping.getPollutantTagKey());

            // POLLUTANT_GROUP
            this.insertSampleValuesStmnt.setLongAtName("POLLUTANT_GROUP_ID", parameterMapping.getPollutantGroupTagId());
            aggregationValue.setPollutantgroupKey(parameterMapping.getPollutantGroupKey());

            // SAMPLE_DATE
            this.insertSampleValuesStmnt.setDateAtName("SAMPLE_DATE", new java.sql.Date(sampleDate.getTime()));
            aggregationValue.setMaxDate(sampleDate);
            aggregationValue.setMinDate(sampleDate);

            // SAMPLE_VALUE
            final float SAMPLE_VALUE = ((Double)sampleValueObject).floatValue();
            this.insertSampleValuesStmnt.setFloatAtName("SAMPLE_VALUE", SAMPLE_VALUE);
            aggregationValue.setMinValue(SAMPLE_VALUE);
            aggregationValue.setMaxValue(SAMPLE_VALUE);

            // fill the list and eliminate duplicates
            aggregationValues.add(aggregationValue);

            this.insertSampleValuesStmnt.executeUpdate();
            final ResultSet generatedKeys = this.insertSampleValuesStmnt.getGeneratedKeys();
            if ((null != generatedKeys)) {
                while (generatedKeys.next()) {
                    sampeValueIds.add(generatedKeys.getLong(1));
                }
                generatedKeys.close();

                final Parameter parameter = new Parameter(parameterMapping);
                if (!parameters.contains(parameter)) {
                    parameters.add(parameter);
                }
                added++;
            } else {
                log.error("could not fetch generated key for inserted sampleValues for Moss Station " + mossStationId);
            }
        }

        if (added > 0) {
            if (log.isDebugEnabled()) {
                log.debug(added + " of " + i + " sampleValues added for Moss Station " + sampeValueIds.size()
                            + " IDs generated");
            }
        } else {
            log.warn("no supported sampleValues found in " + i + " available sampleValues for Moss Station "
                        + mossStationId);
        }

        return sampeValueIds;
    }

    /**
     * DOCUMENT ME!
     *
     * @throws  IOException   DOCUMENT ME!
     * @throws  SQLException  DOCUMENT ME!
     */
    public void doBootstrap() throws IOException, SQLException {
        final long startTime = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
            log.debug("Cleaning and Bootstrapping Moss Tables");
        }

        final String truncateMossTablesTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/moss/truncate-moss-tables.sql"),
                "UTF-8");
        this.executeBatchStatement(targetConnection, truncateMossTablesTpl);

        final String insertMossTaggroupsTpl = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/moss/bootstrap/insert-moss-taggroups.sql"),
                "UTF-8");
        final Statement insertMossTaggroups = this.targetConnection.createStatement();
        insertMossTaggroups.execute(insertMossTaggroupsTpl);

        this.targetConnection.commit();
        insertMossTaggroups.close();

        log.info("Moss Tables successfully bootstrapped in "
                    + ((System.currentTimeMillis() - startTime) / 1000) + "s");
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  DOCUMENT ME!
     */
    public static void main(final String[] args) {
        final long startTime = System.currentTimeMillis();
        final Logger logger = Logger.getLogger(MossImport.class);

        MossImport mossImport = null;
        try {
            if (args.length == 2) {
                if (logger.isDebugEnabled()) {
                    logger.debug("loading Moss properties from: " + args[0]);
                }
                mossImport = new MossImport(FileSystems.getDefault().getPath(args[0]),
                        FileSystems.getDefault().getPath(args[1]));
            } else {
                mossImport = new MossImport();
            }

            mossImport.doBootstrap();
            logger.info("Moss Indeximport successfully initialized and bootstrapped in "
                        + ((System.currentTimeMillis() - startTime) / 1000) + "s");

            logger.info("Starting Moss Import ......");
            final int stations = mossImport.doImport();

            logger.info(stations + " Moss Stations successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
        } catch (Exception ex) {
            logger.error("could not create Moss import instance: " + ex.getMessage(), ex);
        } finally {
            try {
                if (mossImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing source connection");
                    }
                    mossImport.sourceConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close source connection", ex);
                System.exit(1);
            }

            try {
                if (mossImport != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("closing target connection");
                    }
                    mossImport.targetConnection.close();
                }
            } catch (SQLException ex) {
                logger.error("could not close target connection", ex);
                System.exit(1);
            }
        }
    }
}
