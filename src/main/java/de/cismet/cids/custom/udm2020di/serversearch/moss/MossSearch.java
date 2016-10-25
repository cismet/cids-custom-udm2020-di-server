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

import java.util.HashMap;

import de.cismet.cids.custom.udm2020di.serversearch.AbstractMaxValuesSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class MossSearch extends AbstractMaxValuesSearch {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger LOGGER = Logger.getLogger(MossSearch.class);
    protected static final HashMap<String, String> PARAMETER_MAPPING = new HashMap<String, String>();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new MossSearch object.
     *
     * @throws  IOException  DOCUMENT ME!
     */
    public MossSearch() throws IOException {
        this.searchTpl = IOUtils.toString(MossSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/moss/moss-search.tpl.sql"),
                "UTF-8");
        this.maxSampleValueConditionTpl = IOUtils.toString(MossSearch.class.getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/serversearch/moss/max-sample-value-condition.tpl.sql"),
                "UTF-8");
    }
}
