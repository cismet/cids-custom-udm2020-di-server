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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.Date;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
@JacksonXmlRootElement
public class AggregationValue {

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

    //~ Methods ----------------------------------------------------------------

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
}
