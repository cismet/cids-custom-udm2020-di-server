/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.cismet.cids.custom.udm2020di.types;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.io.Serializable;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.annotation.XmlTransient;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
@JacksonXmlRootElement
public class AggregationValue implements Serializable, Cloneable, Comparable<AggregationValue> {

    //~ Static fields/initializers ---------------------------------------------

    @JsonIgnore
    @XmlTransient
    public static final char UNIT_SEPARATOR = Parameter.UNIT_SEPARATOR;
    @JsonIgnore
    @XmlTransient
    public static final Pattern UNIT_REGEX = Parameter.UNIT_REGEX;

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty
    private String name;

    @JacksonXmlProperty(localName = "pollutantkey")
    @JsonProperty("pollutantkey")
    private String pollutantKey;

    @JacksonXmlProperty(localName = "pollutantgroupkey")
    @JsonProperty("pollutantgroupkey")
    private String pollutantgroupKey;

    @JacksonXmlProperty(localName = "mindate")
    @JsonProperty("mindate")
    private Date minDate;

    @JacksonXmlProperty(localName = "maxdate")
    @JsonProperty("maxdate")
    private Date maxDate;

    @JacksonXmlProperty(localName = "minvalue")
    @JsonProperty("minvalue")
    private float minValue;

    @JacksonXmlProperty(localName = "maxvalue")
    @JsonProperty("maxvalue")
    private float maxValue;

    @JacksonXmlProperty(localName = "unit")
    @JsonProperty("unit")
    private String unit;

    @JacksonXmlProperty(localName = "probepk")
    @JsonProperty("probepk")
    private String probePk;

    @JacksonXmlProperty(localName = "releasetype")
    @JsonProperty("releasetype")
    private String releaseType;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new AggregationValue object.
     */
    public AggregationValue() {
    }

    /**
     * Creates a new AggregationValue object.
     *
     * @param  name               DOCUMENT ME!
     * @param  unit               DOCUMENT ME!
     * @param  probePk            DOCUMENT ME!
     * @param  releaseType        DOCUMENT ME!
     * @param  pollutantKey       DOCUMENT ME!
     * @param  pollutantgroupKey  DOCUMENT ME!
     * @param  minDate            DOCUMENT ME!
     * @param  maxDate            DOCUMENT ME!
     * @param  minValue           DOCUMENT ME!
     * @param  maxValue           DOCUMENT ME!
     */
    public AggregationValue(final String name,
            final String unit,
            final String probePk,
            final String releaseType,
            final String pollutantKey,
            final String pollutantgroupKey,
            final Date minDate,
            final Date maxDate,
            final float minValue,
            final float maxValue) {
        this.name = name;
        this.unit = unit;
        this.probePk = probePk;
        this.releaseType = releaseType;
        this.pollutantKey = pollutantKey;
        this.pollutantgroupKey = pollutantgroupKey;
        this.minDate = minDate;
        this.maxDate = maxDate;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Get the value of probePk.
     *
     * @return  the value of probePk
     */
    public String getProbePk() {
        return probePk;
    }

    /**
     * Set the value of probePk.
     *
     * @param  probePk  new value of probePk
     */
    public void setProbePk(final String probePk) {
        this.probePk = probePk;
    }

    /**
     * Set the value of unit.
     *
     * @param  unit  new value of unit
     */
    public void setUnit(final String unit) {
        this.unit = unit;
    }
    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    @XmlTransient
    @JsonIgnore
    public String getPlainName() {
        final String paramUnit = this.getUnit();
        if ((paramUnit != null) && !paramUnit.isEmpty()) {
            if ((this.getName() != null) && !this.getName().isEmpty()) {
                final String tmpName = this.getName().replace(paramUnit, "").trim();
                final int index = tmpName.indexOf(UNIT_SEPARATOR);
                if (index != -1) {
                    return tmpName.substring(0, index).trim();
                }
                return tmpName;
            }
        }
        return this.getName();
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getUnit() {
        if ((this.unit == null) || this.unit.isEmpty()) {
            if ((this.getName() != null) && !this.getName().isEmpty()) {
                final Matcher matcher = UNIT_REGEX.matcher(this.getName());
                if (matcher.find()) {
                    this.unit = matcher.group();
                }
            }
        }
        return this.unit;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getName() {
        return name;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  name  DOCUMENT ME!
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getPollutantKey() {
        return pollutantKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  pollutantKey  DOCUMENT ME!
     */
    public void setPollutantKey(final String pollutantKey) {
        this.pollutantKey = pollutantKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getPollutantgroupKey() {
        return pollutantgroupKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  pollutantgroupKey  DOCUMENT ME!
     */
    public void setPollutantgroupKey(final String pollutantgroupKey) {
        this.pollutantgroupKey = pollutantgroupKey;
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
     * @param  minDate  DOCUMENT ME!
     */
    public void setMinDate(final Date minDate) {
        this.minDate = minDate;
    }

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
     * @param  maxDate  DOCUMENT ME!
     */
    public void setMaxDate(final Date maxDate) {
        this.maxDate = maxDate;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public float getMinValue() {
        return minValue;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  minValue  DOCUMENT ME!
     */
    public void setMinValue(final float minValue) {
        this.minValue = minValue;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public float getMaxValue() {
        return maxValue;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  maxValue  DOCUMENT ME!
     */
    public void setMaxValue(final float maxValue) {
        this.maxValue = maxValue;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getReleaseType() {
        return releaseType;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  releaseType  DOCUMENT ME!
     */
    public void setReleaseType(final String releaseType) {
        this.releaseType = releaseType;
    }

    @Override
    public int compareTo(final AggregationValue aggregationValue) {
        return this.getName().compareTo(aggregationValue.getName());
    }

    @Override
    public String toString() {
        return this.getName();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        super.clone();
        return new AggregationValue(
                this.name,
                this.getUnit(),
                this.probePk,
                this.releaseType,
                this.pollutantKey,
                this.pollutantgroupKey,
                (this.minDate != null) ? (Date)this.minDate.clone() : null,
                (this.maxDate != null) ? (Date)this.maxDate.clone() : null,
                this.minValue,
                this.maxValue);
    }
}
