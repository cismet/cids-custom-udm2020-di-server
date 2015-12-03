/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

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
@JsonFormat(shape = JsonFormat.Shape.OBJECT)
@JsonAutoDetect(
    fieldVisibility = JsonAutoDetect.Visibility.NONE,
    isGetterVisibility = JsonAutoDetect.Visibility.NONE,
    getterVisibility = JsonAutoDetect.Visibility.NONE,
    setterVisibility = JsonAutoDetect.Visibility.NONE
)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregationValues extends AbstractCollection<AggregationValue> implements Serializable, Cloneable {

    //~ Instance fields --------------------------------------------------------

    private final TreeMap<String, AggregationValue> aggregationValues = new TreeMap<String, AggregationValue>();

    @JsonProperty
    @Getter
    private Date minDate = null;

    @JsonProperty
    @Getter
    private Date maxDate = null;

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

    /**
     * Creates a new AggregationValues object.
     *
     * @param  aggregationValues  DOCUMENT ME!
     * @param  minDate            DOCUMENT ME!
     * @param  maxDate            DOCUMENT ME!
     */
    @JsonCreator
    public AggregationValues(@JsonProperty("aggregationValues") final Collection<AggregationValue> aggregationValues,
            @JsonProperty("minDate") final Date minDate,
            @JsonProperty("maxDate") final Date maxDate) {
        this.addAll(aggregationValues);
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    /**
     * Creates a new AggregationValues object.
     *
     * @param  aggregationValues  DOCUMENT ME!
     */
    protected AggregationValues(final AggregationValues aggregationValues) {
        this.aggregationValues.putAll(aggregationValues.aggregationValues);
        this.minDate = aggregationValues.minDate;
        this.maxDate = aggregationValues.maxDate;
    }

    //~ Methods ----------------------------------------------------------------

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
                    aggregationValue.getMaxDate(),
                    aggregationValue.getMaxDate(),
                    aggregationValue.getMaxValue(),
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

    @JsonProperty(value = "aggregationValues")
    @Override
    public AggregationValue[] toArray() {
        return super.toArray(new AggregationValue[this.size()]);
    }

    @Override
    public AggregationValues clone() throws CloneNotSupportedException {
        return new AggregationValues(this);
    }
}
