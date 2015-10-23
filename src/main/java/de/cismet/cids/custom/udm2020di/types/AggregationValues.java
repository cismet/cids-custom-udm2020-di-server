/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types;

import java.io.Serializable;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class AggregationValues extends AbstractCollection<AggregationValue> implements Serializable {

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

    /**
     * DOCUMENT ME!
     *
     * @param   c  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public boolean addAllMax(final Collection<? extends AggregationValue> c) {
        boolean modified = false;
        for (final AggregationValue e : c) {
            if (this.addMax(e)) {
                modified = true;
            }
        }
        return modified;
    }

    /**
     * Add only maximum values and set the minimum of the maximum values.
     *
     * @param   aggregationValue  DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public boolean addMax(final AggregationValue aggregationValue) {
        // set global max / min dates
        maxDate = ((maxDate == null) || aggregationValue.getMaxDate().after(maxDate)) ? aggregationValue.getMaxDate()
                                                                                      : maxDate;
        minDate = ((minDate == null) || aggregationValue.getMaxDate().before(minDate)) ? aggregationValue.getMaxDate()
                                                                                       : minDate;
        // group EPRTR aggregation values by release type
        final String aggregationKey =
            ((aggregationValue.getReleaseType() != null)
                        && !aggregationValue.getReleaseType().isEmpty())
            ? (aggregationValue.getReleaseType() + '.' + aggregationValue.getPollutantKey())
            : aggregationValue.getPollutantKey();

        if (aggregationValues.containsKey(aggregationKey)) {
            final AggregationValue existingAggregationValue = aggregationValues.get(aggregationKey);

            // set new max values and corresponding date of max value (not max date!)
            if (aggregationValue.getMaxValue() > existingAggregationValue.getMaxValue()) {
                existingAggregationValue.setMaxValue(aggregationValue.getMaxValue());
                existingAggregationValue.setMaxDate(aggregationValue.getMaxDate());
                // existingAggregationValue.setMinDate(aggregationValue.getMinDate());
            }

            // set new min(max) values and corresponding date of min value (not min date!)
            if (aggregationValue.getMaxValue() < existingAggregationValue.getMinValue()) {
                existingAggregationValue.setMinValue(aggregationValue.getMaxValue());
                // existingAggregationValue.setMaxDate(aggregationValue.getMaxDate());
                existingAggregationValue.setMinDate(aggregationValue.getMaxDate());
            }

//            maxDate = maxDate.before(aggregationValue.getMaxDate())
//                    ? aggregationValue.getMaxDate() : maxDate;
//            minDate = minDate.after(aggregationValue.getMinDate())
//                    ? aggregationValue.getMinDate() : minDate;

            return false;
        } else {
            aggregationValues.put(
                aggregationKey,
                new AggregationValue(
                    aggregationValue.getName(),
                    aggregationValue.getUnit(),
                    aggregationValue.getProbePk(),
                    aggregationValue.getReleaseType(),
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
    public boolean add(final AggregationValue aggregationValue) {
        // set global max / min dates
        maxDate = ((maxDate == null) || aggregationValue.getMaxDate().after(maxDate)) ? aggregationValue.getMaxDate()
                                                                                      : maxDate;
        minDate = ((minDate == null) || aggregationValue.getMinDate().before(minDate)) ? aggregationValue.getMinDate()
                                                                                       : minDate;
        // group EPRTR aggregation values by release type
        final String aggregationKey =
            ((aggregationValue.getReleaseType() != null)
                        && !aggregationValue.getReleaseType().isEmpty())
            ? (aggregationValue.getReleaseType() + '.' + aggregationValue.getPollutantKey())
            : aggregationValue.getPollutantKey();

        if (aggregationValues.containsKey(aggregationKey)) {
            final AggregationValue existingAggregationValue = aggregationValues.get(aggregationKey);

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
            aggregationValues.put(
                aggregationKey,
                new AggregationValue(
                    aggregationValue.getName(),
                    aggregationValue.getUnit(),
                    aggregationValue.getProbePk(),
                    aggregationValue.getReleaseType(),
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

    @Override
    public void clear() {
        super.clear();
        this.aggregationValues.clear();
        this.maxDate = null;
        this.minDate = null;
    }
}
