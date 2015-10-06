/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types.wa;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import javax.xml.bind.annotation.XmlRootElement;

import de.cismet.cids.custom.udm2020di.serializers.FlexibleFloatDeserializer;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@XmlRootElement
@JsonIgnoreProperties(ignoreUnknown = true)
public class OwMessstelle extends Messstelle {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private String operativ;

    @JacksonXmlProperty(isAttribute = true)
    private String gewaessername;

    @JsonProperty("gewaesserezk")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "gewaesserezk"
    )
    private String gewaesserEzk;

    @JsonProperty("ezggroesse")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "ezggroesse"
    )
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float ezgGroesse;

    //~ Methods ----------------------------------------------------------------

    /**
     * Get the value of operativ.
     *
     * @return  the value of operativ
     */
    public String getOperativ() {
        return operativ;
    }

    /**
     * Set the value of operativ.
     *
     * @param  operativ  new value of operativ
     */
    public void setOperativ(final String operativ) {
        this.operativ = operativ;
    }

    /**
     * Get the value of gewaesserName.
     *
     * @return  the value of gewaesserName
     */
    public String getGewaessername() {
        return gewaessername;
    }

    /**
     * Set the value of gewaesserName.
     *
     * @param  gewaessername  new value of gewaesserName
     */
    public void setGewaesserName(final String gewaessername) {
        this.gewaessername = gewaessername;
    }

    /**
     * Get the value of gewaesserEzk.
     *
     * @return  the value of gewaesserEzk
     */
    public String getGewaesserEzk() {
        return gewaesserEzk;
    }

    /**
     * Set the value of gewaesserEzk.
     *
     * @param  gewaesserEzk  new value of gewaesserEzk
     */
    public void setGewaesserEzk(final String gewaesserEzk) {
        this.gewaesserEzk = gewaesserEzk;
    }

    /**
     * Get the value of ezgGroesse.
     *
     * @return  the value of ezgGroesse
     */
    public float getEzgGroesse() {
        return ezgGroesse;
    }

    /**
     * Set the value of ezgGroesse.
     *
     * @param  ezgGroesse  new value of ezgGroesse
     */
    public void setEzgGroesse(final float ezgGroesse) {
        this.ezgGroesse = ezgGroesse;
    }
}
