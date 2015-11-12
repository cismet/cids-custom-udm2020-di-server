/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.moss;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import de.cismet.cids.custom.udm2020di.serversearch.AbstractAggregationValuesSearch;
import de.cismet.cids.custom.udm2020di.serversearch.boris.BorisAggregationValuesSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */

public class MossAggregationValuesSearch extends AbstractAggregationValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger LOGGER = Logger.getLogger(MossAggregationValuesSearch.class);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AggregationValuesTagSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public MossAggregationValuesSearch() throws IOException {
        super(IOUtils.toString(
                BorisAggregationValuesSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/moss/get-postfilter-aggregation-values.tpl.sql"),
                "UTF-8"));
    }
}
