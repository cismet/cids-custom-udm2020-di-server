/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types.boris;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import de.cismet.cids.custom.udm2020di.serializers.FlexibleFloatDeserializer;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.Tag;

/**
 * DOCUMENT ME!
 *
 * @author   pd
 * @version  $Revision$, $Date$
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
public class Standort {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private String pk;

    @JacksonXmlProperty(isAttribute = true)
    private String standortbezeichnung;

    @JacksonXmlProperty(isAttribute = true)
    private String standortnummer;

    @JacksonXmlProperty(isAttribute = true)
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float hochwert;

    @JacksonXmlProperty(isAttribute = true)
    private String institut;

    @JacksonXmlProperty(isAttribute = true)
    private String literatur;

    @JacksonXmlProperty(isAttribute = true)
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float rechtswert;

    @JacksonXmlProperty
    private List<Standortparameter> standortparameter;

    @JacksonXmlProperty
    private List<Probenparameter> probenparameter;

    @JacksonXmlProperty
    private List<Tag> tags;

    @JacksonXmlProperty(localName = "aggregationvalues")
    @JsonProperty("aggregationvalues")
    private List<AggregationValue> aggregationValues;

    @JacksonXmlProperty
    private List<String> proben;

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getPk() {
        return pk;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  pk  DOCUMENT ME!
     */
    public void setPk(final String pk) {
        this.pk = pk;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getStandortbezeichnung() {
        return standortbezeichnung;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  standortbezeichnung  DOCUMENT ME!
     */
    public void setStandortbezeichnung(final String standortbezeichnung) {
        this.standortbezeichnung = standortbezeichnung;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getStandortnummer() {
        return standortnummer;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  standortnummer  DOCUMENT ME!
     */
    public void setStandortnummer(final String standortnummer) {
        this.standortnummer = standortnummer;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public float getHochwert() {
        return hochwert;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  hochwert  DOCUMENT ME!
     */
    public void setHochwert(final float hochwert) {
        this.hochwert = hochwert;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getInstitut() {
        return institut;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  institut  DOCUMENT ME!
     */
    public void setInstitut(final String institut) {
        this.institut = institut;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getLiteratur() {
        return literatur;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  literatur  DOCUMENT ME!
     */
    public void setLiteratur(final String literatur) {
        this.literatur = literatur;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public float getRechtswert() {
        return rechtswert;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  rechtswert  DOCUMENT ME!
     */
    public void setRechtswert(final float rechtswert) {
        this.rechtswert = rechtswert;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public List<Standortparameter> getStandortparameter() {
        return standortparameter;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  standortparameter  DOCUMENT ME!
     */
    public void setStandortparameter(final List<Standortparameter> standortparameter) {
        this.standortparameter = standortparameter;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public List<Probenparameter> getProbenparameter() {
        return probenparameter;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  probenparameter  DOCUMENT ME!
     */
    public void setProbenparameter(final List<Probenparameter> probenparameter) {
        this.probenparameter = probenparameter;

        if ((this.probenparameter != null) && !this.probenparameter.isEmpty()) {
            Collections.sort(this.probenparameter);
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public List<Tag> getTags() {
        return tags;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  tags  DOCUMENT ME!
     */
    public void setTags(final List<Tag> tags) {
        this.tags = tags;

        if ((this.tags != null) && !this.tags.isEmpty()) {
            Collections.sort(this.tags);
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public List<AggregationValue> getAggregationValues() {
        return aggregationValues;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  aggregationValues  DOCUMENT ME!
     */
    public void setAggregationValues(final List<AggregationValue> aggregationValues) {
        this.aggregationValues = aggregationValues;

        if ((this.aggregationValues != null) && !this.aggregationValues.isEmpty()) {
            Collections.sort(this.aggregationValues);
        }
    }

    /**
     * Get the value of proben.
     *
     * @return  the value of proben
     */
    public List<String> getProben() {
        return proben;
    }

    /**
     * Set the value of proben.
     *
     * @param  proben  new value of proben
     */
    public void setProben(final List<String> proben) {
        this.proben = proben;

        if ((this.proben != null) && !this.proben.isEmpty()) {
            Collections.sort(this.proben);
        }
    }
}
