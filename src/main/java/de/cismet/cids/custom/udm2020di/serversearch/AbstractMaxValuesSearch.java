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

import lombok.Getter;
import lombok.Setter;

import org.apache.log4j.Logger;

import java.math.BigDecimal;

import java.rmi.RemoteException;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

import de.cismet.cidsx.base.types.Type;

import de.cismet.cidsx.server.api.types.SearchInfo;
import de.cismet.cidsx.server.api.types.SearchParameterInfo;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public abstract class AbstractMaxValuesSearch extends AbstractCidsServerSearch implements CustomMaxValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

    protected static final String DOMAIN = "UDM2020-DI";

    protected static Logger LOGGER = Logger.getLogger(AbstractMaxValuesSearch.class);

    //~ Instance fields --------------------------------------------------------

    protected String maxSampleValueConditionTpl;
    protected String searchTpl;

    @Getter @Setter protected Map<String, Float> maxValues;

    @Getter @Setter private Collection<Integer> objectIds;

    @Getter @Setter private int classId = -1;

    @Getter @Setter private Date minDate;

    @Getter @Setter private Date maxDate;

    @Getter private final SearchInfo searchInfo;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AbstractMaxValuesSearch object.
     */
    protected AbstractMaxValuesSearch() {
        searchInfo = new SearchInfo();
        searchInfo.setKey(this.getClass().getName());
        searchInfo.setName(this.getClass().getSimpleName());
        searchInfo.setDescription(this.getClass().getSimpleName()
                    + "Post Filter Search Search for Nodes by date an may values");

        final List<SearchParameterInfo> parameterDescription = new LinkedList<SearchParameterInfo>();
        SearchParameterInfo searchParameterInfo;

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setDescription("class ID of the objects to be filtered");
        searchParameterInfo.setKey("classId");
        searchParameterInfo.setType(Type.INTEGER);
        parameterDescription.add(searchParameterInfo);

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setKey("objectIds");
        searchParameterInfo.setDescription("IDs of the objects to be filtered");
        searchParameterInfo.setType(Type.JAVA_CLASS);
        // searchParameterInfo.setArray(true);
        searchParameterInfo.setAdditionalTypeInfo("java.util.Collection<Integer>");
        parameterDescription.add(searchParameterInfo);

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setKey("maxValues");
        searchParameterInfo.setDescription("map of max values");
        searchParameterInfo.setType(Type.INTEGER);
        searchParameterInfo.setAdditionalTypeInfo("java.util.Map<String, Float>");
        parameterDescription.add(searchParameterInfo);

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setKey("minDate");
        searchParameterInfo.setDescription("minDate");
        searchParameterInfo.setType(Type.DATE);
        parameterDescription.add(searchParameterInfo);

        searchParameterInfo = new SearchParameterInfo();
        searchParameterInfo.setKey("maxDate");
        searchParameterInfo.setType(Type.DATE);
        parameterDescription.add(searchParameterInfo);

        searchInfo.setParameterDescription(parameterDescription);

        final SearchParameterInfo resultParameterInfo = new SearchParameterInfo();
        resultParameterInfo.setKey("return");
        // resultParameterInfo.setArray(true);
        resultParameterInfo.setType(Type.NODE);
        searchInfo.setResultDescription(resultParameterInfo);
    }

    //~ Methods ----------------------------------------------------------------

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
            objectIdsBuilder.append(objectIdsIterator.next());
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

                    final MetaObjectNode node = new MetaObjectNode(DOMAIN, objectID, classID, name, null, null); // TODO: Check4CashedGeomAndLightweightJson

                    result.add(node);
                }
            } catch (RemoteException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new SearchException(ex.getMessage(), ex);
            }
        } else {
            final String message = "active local server " + DOMAIN + "not found";
            LOGGER.error(message); // NOI18N
            throw new SearchException(message);
        }

        LOGGER.info(result.size() + " objects found in "
                    + (System.currentTimeMillis() - startTime) + "ms");
        return result;
    }
}
