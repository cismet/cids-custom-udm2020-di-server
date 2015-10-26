/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.serversearch;

import java.util.Collection;
import java.util.Date;
import java.util.Map;

import de.cismet.cids.server.search.MetaObjectNodeServerSearch;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public interface CustomMaxValuesSearch extends MetaObjectNodeServerSearch {

    //~ Methods ----------------------------------------------------------------

    /**
     * Get the value of maxValues.
     *
     * @return  the value of maxValues
     */
    Map<String, Float> getMaxValues();

    /**
     * Set the value of maxValues.
     *
     * @param  maxValues  new value of maxValues
     */
    void setMaxValues(final Map<String, Float> maxValues);

    /**
     * Get the value of minDate.
     *
     * @return  the value of minDate
     */
    Date getMinDate();

    /**
     * Set the value of minDate.
     *
     * @param  minDate  new value of minDate
     */
    void setMinDate(final Date minDate);

    /**
     * Get the value of maxDate.
     *
     * @return  the value of maxDate
     */
    Date getMaxDate();

    /**
     * Set the value of maxDate.
     *
     * @param  maxDate  new value of maxDate
     */
    void setMaxDate(final Date maxDate);

    /**
     * Get the value of objectIds.
     *
     * @return  the value of objectIds
     */
    Collection<Integer> getObjectIds();

    /**
     * Set the value of objectIds.
     *
     * @param  objectIds  new value of objectIds
     */
    void setObjectIds(final Collection<Integer> objectIds);

    /**
     * Get the value of classId.
     *
     * @return  the value of classId
     */
    int getClassId();

    /**
     * Set the value of classId.
     *
     * @param  classId  new value of classId
     */
    void setClassId(final int classId);
}
