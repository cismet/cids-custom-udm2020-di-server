/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch;

import Sirius.server.middleware.interfaces.domainserver.MetaService;
import Sirius.server.middleware.types.MetaObjectNode;

import org.apache.log4j.Logger;

import java.math.BigDecimal;

import java.rmi.RemoteException;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public abstract class AbstractMaxValuesSearch extends AbstractCidsServerSearch implements CustomMaxValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("YYYYMMdd");

    protected static final String DOMAIN = "UDM2020-DI";

    protected static Logger LOGGER = Logger.getLogger(AbstractMaxValuesSearch.class);

    //~ Instance fields --------------------------------------------------------

    protected Map<String, Float> maxValues;

    protected String maxSampleValueConditionTpl;
    protected String searchTpl;

    private Collection<Integer> objectIds;

    private int classId = -1;

    private Date minDate;

    private Date maxDate;

    //~ Methods ----------------------------------------------------------------

    /**
     * Get the value of minDate.
     *
     * @return  the value of minDate
     */
    @Override
    public Date getMinDate() {
        return minDate;
    }

    /**
     * Set the value of minDate.
     *
     * @param  minDate  new value of minDate
     */
    @Override
    public void setMinDate(final Date minDate) {
        this.minDate = minDate;
    }

    /**
     * Get the value of maxDate.
     *
     * @return  the value of maxDate
     */
    @Override
    public Date getMaxDate() {
        return maxDate;
    }

    /**
     * Set the value of maxDate.
     *
     * @param  maxDate  new value of maxDate
     */
    @Override
    public void setMaxDate(final Date maxDate) {
        this.maxDate = maxDate;
    }

    /**
     * DOCUMENT ME!
     *
     * @param   classId    DOCUMENT ME!
     * @param   objectIds  DOCUMENT ME!
     * @param   maxValues  DOCUMENT ME!
     * @param   minDate    DOCUMENT ME!
     * @param   maxDate    DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createSearchStatement(
            final int classId,
            final Collection<Integer> objectIds,
            final Map<String, Float> maxValues,
            final Date minDate,
            final Date maxDate) {
        final StringBuilder customSearchStatementBuilder = new StringBuilder();
        final StringBuilder objectIdsBuilder = new StringBuilder();
        final Iterator<Integer> objectIdsIterator = objectIds.iterator();
        while (objectIdsIterator.hasNext()) {
            objectIdsBuilder.append('\'').append(objectIdsIterator.next()).append('\'');
            if (objectIdsIterator.hasNext()) {
                objectIdsBuilder.append(',');
            }
        }

        // final StringBuilder maxValuesBuilder = new StringBuilder();
        final Iterator<Entry<String, Float>> maxValuesIterator = maxValues.entrySet().iterator();
        while (maxValuesIterator.hasNext()) {
            final Entry<String, Float> maxValue = maxValuesIterator.next();

            String maxSampleValueConditionStatement;
            maxSampleValueConditionStatement = this.maxSampleValueConditionTpl.replace("%TAG_KEY%", maxValue.getKey());
            maxSampleValueConditionStatement = maxSampleValueConditionStatement.replace(
                    "%MAX_VALUE%",
                    String.valueOf(maxValue.getValue()));
            // maxValuesBuilder.append(maxSampleValueConditionStatement);
            // if (maxValuesIterator.hasNext()) {
            // maxValuesBuilder.append(" \n OR ");
            // }
            String customSearchStatement = this.searchTpl.replace(
                    "%CLASS_ID%",
                    String.valueOf(classId));
            customSearchStatement = customSearchStatement.replace(
                    "%OBJECT_IDS%",
                    objectIdsBuilder);
            customSearchStatement = customSearchStatement.replace(
                    "%MAX_SAMPLE_VALUE_CONDITION%",
                    maxSampleValueConditionStatement);
            // customSearchStatement = customSearchStatement.replace(
            // "%NUM_MAX_SAMPLE_VALUE_CONDITIONS%",
            // String.valueOf(maxValues.size()));
            customSearchStatement = customSearchStatement.replace(
                    "%MIN_DATE%",
                    DATE_FORMAT.format(minDate));
            customSearchStatement = customSearchStatement.replace(
                    "%MAX_DATE%",
                    DATE_FORMAT.format(maxDate));

            customSearchStatementBuilder.append(customSearchStatement);
            if (maxValuesIterator.hasNext()) {
                customSearchStatementBuilder.append(" \nINTERSECT \n");
            } else {
                customSearchStatementBuilder.append(" \nORDER BY NAME");
            }
        }

        return customSearchStatementBuilder.toString();
    }

    /**
     * Get the value of maxValues.
     *
     * @return  the value of maxValues
     */
    @Override
    public Map<String, Float> getMaxValues() {
        return maxValues;
    }

    /**
     * Set the value of maxValues.
     *
     * @param  maxValues  new value of maxValues
     */
    @Override
    public void setMaxValues(final Map<String, Float> maxValues) {
        this.maxValues = maxValues;
    }

    /**
     * Get the value of objectIds.
     *
     * @return  the value of objectIds
     */
    @Override
    public Collection<Integer> getObjectIds() {
        return objectIds;
    }

    /**
     * Set the value of objectIds.
     *
     * @param  objectIds  new value of objectIds
     */
    @Override
    public void setObjectIds(final Collection<Integer> objectIds) {
        this.objectIds = objectIds;
    }

    /**
     * Get the value of classId.
     *
     * @return  the value of classId
     */
    @Override
    public int getClassId() {
        return classId;
    }

    /**
     * Set the value of classId.
     *
     * @param  classId  new value of classId
     */
    @Override
    public void setClassId(final int classId) {
        this.classId = classId;
    }

    @Override
    public Collection<MetaObjectNode> performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();
        if ((this.minDate == null) || (this.maxDate == null)
                    || (this.objectIds == null) || this.objectIds.isEmpty()
                    || (this.maxValues == null) || this.maxValues.isEmpty()
                    || (this.classId == -1)) {
            LOGGER.warn("missing parameters, returning empty collection");
        } else {
            LOGGER.info("performing search for " + this.objectIds.size() + " objects of class #"
                        + this.classId + " and " + this.maxValues.size() + " max values between "
                        + DATE_FORMAT.format(minDate) + " and " + DATE_FORMAT.format(maxDate));
        }

        final Collection<MetaObjectNode> result = new ArrayList<MetaObjectNode>();
        final String customSearchStatement = this.createSearchStatement(
                classId,
                objectIds,
                maxValues,
                minDate,
                maxDate);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(customSearchStatement);
        }

        final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
        if (metaService != null) {
            try {
                final ArrayList<ArrayList> resultSet = metaService.performCustomSearch(customSearchStatement);

                for (final ArrayList row : resultSet) {
                    final int classID = ((BigDecimal)row.get(0)).intValue();
                    final int objectID = ((BigDecimal)row.get(1)).intValue();
                    final String name = (String)row.get(2);

                    final MetaObjectNode node = new MetaObjectNode(DOMAIN, objectID, classID, name);

                    result.add(node);
                }
            } catch (RemoteException ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        } else {
            LOGGER.error("active local server " + DOMAIN + "not found"); // NOI18N
        }

        LOGGER.info(result.size() + " objects found in "
                    + (System.currentTimeMillis() - startTime) + "ms");
        return result;
    }
}
