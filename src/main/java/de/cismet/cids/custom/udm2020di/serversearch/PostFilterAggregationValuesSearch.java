/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch;

import java.util.Collection;

import de.cismet.cids.server.search.CidsServerSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */

public interface PostFilterAggregationValuesSearch extends CidsServerSearch {

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    Collection<Integer> getObjectIds();

    /**
     * DOCUMENT ME!
     *
     * @param  objectIds  DOCUMENT ME!
     */
    void setObjectIds(final Collection<Integer> objectIds);
}
