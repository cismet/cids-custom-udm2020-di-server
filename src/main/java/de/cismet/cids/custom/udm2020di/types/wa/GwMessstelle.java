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
public class GwMessstelle extends Messstelle {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private String messtellenart;

    @JacksonXmlProperty(
        isAttribute = true,
        localName = "gwkkame"
    )
    @JsonProperty("gwkname")
    private String gwkName;

    @JacksonXmlProperty(isAttribute = true)
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float tiefe;

    //~ Methods ----------------------------------------------------------------

    /**
     * Get the value of messtellenart.
     *
     * @return  the value of messtellenart
     */
    public String getMesstellenart() {
        return messtellenart;
    }

    /**
     * Set the value of messtellenart.
     *
     * @param  messtellenart  new value of messtellenart
     */
    public void setMesstellenart(final String messtellenart) {
        this.messtellenart = messtellenart;
    }

    /**
     * Get the value of gwkName.
     *
     * @return  the value of gwkName
     */
    public String getGwkName() {
        return gwkName;
    }

    /**
     * Set the value of gwkName.
     *
     * @param  gwkName  new value of gwkName
     */
    public void setGwkName(final String gwkName) {
        this.gwkName = gwkName;
    }

    /**
     * Get the value of tiefe.
     *
     * @return  the value of tiefe
     */
    public float getTiefe() {
        return tiefe;
    }

    /**
     * Set the value of tiefe.
     *
     * @param  tiefe  new value of tiefe
     */
    public void setTiefe(final float tiefe) {
        this.tiefe = tiefe;
    }
}
