/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch;

import Sirius.server.middleware.impls.domainserver.DomainServerImpl;

import lombok.Getter;
import lombok.Setter;

import org.apache.log4j.Logger;

import java.io.IOException;

import java.rmi.RemoteException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Collection;
import java.util.Iterator;

import de.cismet.cids.custom.udm2020di.dataexport.OracleExport;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.AggregationValues;
import de.cismet.cids.custom.udm2020di.types.AggregationValuesBean;

import de.cismet.cids.server.search.AbstractCidsServerSearch;
import de.cismet.cids.server.search.SearchException;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */

public abstract class AbstractAggregationValuesSearch extends AbstractCidsServerSearch
        implements PostFilterAggregationValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final String DOMAIN = "UDM2020-DI";

    protected static Logger LOGGER = Logger.getLogger(AbstractAggregationValuesSearch.class);

    //~ Instance fields --------------------------------------------------------

    @Getter @Setter protected Collection<Integer> objectIds;

    protected final String getAggregationValuesTpl;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new PostFilterAggregationValuesSearch object.
     *
     * @param  getAggregationValuesTpl  DOCUMENT ME!
     */
    protected AbstractAggregationValuesSearch(final String getAggregationValuesTpl) {
        this.getAggregationValuesTpl = getAggregationValuesTpl;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @param   objectIds  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    protected String createAggregationValuesSearchStatement(
            final Collection<Integer> objectIds) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("building sql statement for " + objectIds.size() + " object ids");
        }

        final StringBuilder objectIdsBuilder = new StringBuilder();
        final Iterator<Integer> objectIdsIterator = objectIds.iterator();
        while (objectIdsIterator.hasNext()) {
            // objectIdsBuilder.append('\'').append(objectIdsIterator.next()).append('\'');
            objectIdsBuilder.append(objectIdsIterator.next());
            if (objectIdsIterator.hasNext()) {
                objectIdsBuilder.append(',');
            }
        }

        final String getAggregationValuesSearchStatement = this.getAggregationValuesTpl.replace(
                "%OBJECT_IDS%",
                objectIdsBuilder);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(getAggregationValuesSearchStatement);
        }

        return getAggregationValuesSearchStatement;
    }

    @Override
    public Collection performServerSearch() throws SearchException {
        final long startTime = System.currentTimeMillis();
        final AggregationValues aggregationValues = new AggregationValues();

        if ((this.objectIds != null) && !this.objectIds.isEmpty()) {
            LOGGER.info("performing search for aggregation values of "
                        + this.objectIds.size() + " different objects.");

            final String getAggregationValuesSearchStatement = this.createAggregationValuesSearchStatement(
                    this.objectIds);
//            if (LOGGER.isDebugEnabled()) {
//                LOGGER.debug(getAggregationValuesSearchStatement);
//            }

            // final MetaService metaService = (MetaService)getActiveLocalServers().get(DOMAIN);
            // if (metaService != null) {
            Statement aggregationValuesSearchStmnt = null;
            try {
                final Connection connection = DomainServerImpl.getServerInstance().getConnectionPool().getConnection();
                aggregationValuesSearchStmnt = connection.createStatement();
                final ResultSet aggregationValuesSearchResult = aggregationValuesSearchStmnt.executeQuery(
                        getAggregationValuesSearchStatement);
                int i = 0;
                while (aggregationValuesSearchResult.next()) {
                    final AggregationValuesBean aggregationValuesBean = OracleExport.JSON_MAPPER.readValue(
                            aggregationValuesSearchResult.getClob(1).getCharacterStream(),
                            AggregationValuesBean.class);
                    // ignore all aggregation values that do not map to a concrete pollutant
                    for (final AggregationValue aggregationValue : aggregationValuesBean.getAggregationValues()) {
                        if (!aggregationValue.getPollutantKey().equalsIgnoreCase("METPlus")
                                    && !aggregationValue.getPollutantKey().equalsIgnoreCase("KWSplus")
                                    && !aggregationValue.getPollutantKey().equalsIgnoreCase("PESTplus")
                                    && !aggregationValue.getPollutantKey().equalsIgnoreCase("THGundLSSplus")
                                    && !aggregationValue.getPollutantKey().equalsIgnoreCase("DNMplus")
                                    && !aggregationValue.getPollutantKey().equalsIgnoreCase("SYSSplus")) {
                            aggregationValues.add(aggregationValue);
                        }
                        i++;
                    }
                }

                aggregationValuesSearchResult.close();
                aggregationValuesSearchStmnt.close();

                if (aggregationValues.isEmpty()) {
                    LOGGER.warn("no aggregation values tags found! (" + i + ") aggregation values filtered");
                } else {
                    LOGGER.info(aggregationValues.size() + " supported aggregation values processed of "
                                + i + " available aggregation values for "
                                + this.objectIds.size() + " objects in "
                                + (System.currentTimeMillis() - startTime) + "ms");
                }
            } catch (RemoteException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new SearchException(ex.getMessage(), ex);
            } catch (IOException ex) {
                throw new SearchException(ex.getMessage(), ex);
            } catch (SQLException ex) {
                LOGGER.error(ex.getMessage(), ex);
                try {
                    if (aggregationValuesSearchStmnt != null) {
                        aggregationValuesSearchStmnt.close();
                    }
                } catch (SQLException sx) {
                    LOGGER.error(sx.getMessage(), sx);
                }
                throw new SearchException(ex.getMessage(), ex);
            }
//            } else {
//                LOGGER.error("active local server " + DOMAIN + " not found"); // NOI18N
//            }
//            } else {
//                LOG.error("active local server " + DOMAIN + " not found"); // NOI18N
//            }
//            } else {
//                logger.error("active local server " + DOMAIN + " not found"); // NOI18N
//            }
//            } else {
//                LOG.error("active local server " + DOMAIN + " not found"); // NOI18N
//            }
        } else {
            LOGGER.warn("missing parameters, returning empty collection");
        }

        return aggregationValues;
    }
}
