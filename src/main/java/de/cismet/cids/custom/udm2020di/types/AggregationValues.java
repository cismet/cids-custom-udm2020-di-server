/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
public class AggregationValues extends AbstractCollection<AggregationValue> {

    //~ Instance fields --------------------------------------------------------

    private final Map<String, AggregationValue> aggregationValues = new TreeMap<String, AggregationValue>();

    private Date maxDate = null;
    private Date minDate = null;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AggregationValues object.
     */
    public AggregationValues() {
    }

    /**
     * Creates a new AggregationValues object.
     *
     * @param  aggregationValueCollection  DOCUMENT ME!
     */
    public AggregationValues(final Collection<AggregationValue> aggregationValueCollection) {
        this.addAll(aggregationValueCollection);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public Date getMaxDate() {
        return maxDate;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public Date getMinDate() {
        return minDate;
    }

    @Override
    public boolean add(final AggregationValue aggregationValue) {
        // set global max / min dates
        maxDate = ((maxDate == null) || aggregationValue.getMaxDate().after(maxDate)) ? aggregationValue.getMaxDate()
                                                                                      : maxDate;
        minDate = ((minDate == null) || aggregationValue.getMinDate().before(minDate)) ? aggregationValue.getMinDate()
                                                                                       : minDate;

        if (aggregationValues.containsKey(aggregationValue.getPollutantKey())) {
            final AggregationValue existingAggregationValue = aggregationValues.get(aggregationValue.getPollutantKey());

            // set new max values and corresponding date of max value (not max date!)
            if (aggregationValue.getMaxValue() > existingAggregationValue.getMaxValue()) {
                existingAggregationValue.setMaxValue(aggregationValue.getMaxValue());
                existingAggregationValue.setMaxDate(aggregationValue.getMaxDate());
                // existingAggregationValue.setMinDate(aggregationValue.getMinDate());
            }

            // set new min values and corresponding date of min value (not min date!)
            if (aggregationValue.getMinValue() < existingAggregationValue.getMinValue()) {
                existingAggregationValue.setMinValue(aggregationValue.getMinValue());
                // existingAggregationValue.setMaxDate(aggregationValue.getMaxDate());
                existingAggregationValue.setMinDate(aggregationValue.getMinDate());
            }

//            maxDate = maxDate.before(aggregationValue.getMaxDate())
//                    ? aggregationValue.getMaxDate() : maxDate;
//            minDate = minDate.after(aggregationValue.getMinDate())
//                    ? aggregationValue.getMinDate() : minDate;

            return false;
        } else {
            aggregationValues.put(aggregationValue.getPollutantKey(),
                new AggregationValue(
                    aggregationValue.getName(),
                    aggregationValue.getUnit(),
                    aggregationValue.getProbePk(),
                    aggregationValue.getPollutantKey(),
                    aggregationValue.getPollutantgroupKey(),
                    aggregationValue.getMinDate(),
                    aggregationValue.getMaxDate(),
                    aggregationValue.getMinValue(),
                    aggregationValue.getMaxValue()));
            return true;
        }
    }

    @Override
    public Iterator<AggregationValue> iterator() {
        return aggregationValues.values().iterator();
    }

    @Override
    public int size() {
        return aggregationValues.size();
    }
}
