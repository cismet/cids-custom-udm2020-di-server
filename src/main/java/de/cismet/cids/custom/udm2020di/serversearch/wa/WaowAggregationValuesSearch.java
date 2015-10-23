/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch.wa;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

import de.cismet.cids.custom.udm2020di.serversearch.PostFilterAggregationValuesSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */

public class WaowAggregationValuesSearch extends PostFilterAggregationValuesSearch {

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AggregationValuesTagSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public WaowAggregationValuesSearch() throws IOException {
        super(IOUtils.toString(
                WaowAggregationValuesSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/waow/get-postfilter-aggregation-values.tpl.sql"),
                "UTF-8"));
    }
}
