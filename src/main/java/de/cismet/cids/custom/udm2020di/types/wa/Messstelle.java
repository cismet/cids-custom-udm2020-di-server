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
public class Messstelle {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private String name;

    @JacksonXmlProperty(isAttribute = true)
    private String pk;

    @JacksonXmlProperty(isAttribute = true)
    private String typ;

    @JacksonXmlProperty(isAttribute = true)
    private String status;

    @JsonProperty("zustaendigestelle")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "zustaendigestelle"
    )
    private String zustaendigeStelle;

    @JacksonXmlProperty(isAttribute = true)
    private String bundesland;

    @JsonProperty("xkoordinate")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "xkoordinate"
    )
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float xKoordinate;

    @JsonProperty("ykoordinate")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "ykoordinate"
    )
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private Float yKoordinate;

    //~ Methods ----------------------------------------------------------------

    /**
     * Get the value of name.
     *
     * @return  the value of name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the value of name.
     *
     * @param  name  new value of name
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * Get the value of pk.
     *
     * @return  the value of pk
     */
    public String getPk() {
        return pk;
    }

    /**
     * Set the value of pk.
     *
     * @param  pk  new value of pk
     */
    public void setPk(final String pk) {
        this.pk = pk;
    }

    /**
     * Get the value of typ.
     *
     * @return  the value of typ
     */
    public String getTyp() {
        return typ;
    }

    /**
     * Set the value of typ.
     *
     * @param  typ  new value of typ
     */
    public void setTyp(final String typ) {
        this.typ = typ;
    }

    /**
     * Get the value of status.
     *
     * @return  the value of status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Set the value of status.
     *
     * @param  status  new value of status
     */
    public void setStatus(final String status) {
        this.status = status;
    }

    /**
     * Get the value of zustaendigeStelle.
     *
     * @return  the value of zustaendigeStelle
     */
    public String getZustaendigeStelle() {
        return zustaendigeStelle;
    }

    /**
     * Set the value of zustaendigeStelle.
     *
     * @param  zustaendigeStelle  new value of zustaendigeStelle
     */
    public void setZustaendigeStelle(final String zustaendigeStelle) {
        this.zustaendigeStelle = zustaendigeStelle;
    }

    /**
     * Get the value of bundesland.
     *
     * @return  the value of bundesland
     */
    public String getBundesland() {
        return bundesland;
    }

    /**
     * Set the value of bundesland.
     *
     * @param  bundesland  new value of bundesland
     */
    public void setBundesland(final String bundesland) {
        this.bundesland = bundesland;
    }

    /**
     * Get the value of xKoordinate.
     *
     * @return  the value of xKoordinate
     */
    public float getxKoordinate() {
        return xKoordinate;
    }

    /**
     * Set the value of xKoordinate.
     *
     * @param  xKoordinate  new value of xKoordinate
     */
    public void setxKoordinate(final float xKoordinate) {
        this.xKoordinate = xKoordinate;
    }

    /**
     * Get the value of yKoordinate.
     *
     * @return  the value of yKoordinate
     */
    public Float getyKoordinate() {
        return yKoordinate;
    }

    /**
     * Set the value of yKoordinate.
     *
     * @param  yKoordinate  new value of yKoordinate
     */
    public void setyKoordinate(final Float yKoordinate) {
        this.yKoordinate = yKoordinate;
    }
}
