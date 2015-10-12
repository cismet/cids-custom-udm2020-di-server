/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types.eprtr;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@XmlRootElement
@NoArgsConstructor
@AllArgsConstructor
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Activity implements Cloneable, Comparable<Activity> {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private String name;

    @JsonProperty("notificationperiod")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "notificationperiod"
    )
    private String notificationPeriod;

    @JacksonXmlProperty(isAttribute = true)
    private String code;

    @JacksonXmlProperty(isAttribute = true)
    private String mnemonic;

    @JsonProperty("primaryactivity")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "primaryactivity"
    )
    private String primaryActivity;

    @JsonProperty("productionvolume")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "productionvolume"
    )
    private int productionVolume = -1;

    @JsonProperty("operatinghours")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "operatinghours"
    )
    private int operatingHours = -1;

    @JacksonXmlProperty(isAttribute = true)
    private String product;

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(final Activity activity) {
        return this.toString().compareTo(activity.toString());
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        super.clone();
        return new Activity(
                this.name,
                this.notificationPeriod,
                this.code,
                this.mnemonic,
                this.primaryActivity,
                this.productionVolume,
                this.operatingHours,
                this.product);
    }

    @Override
    public String toString() {
        return (this.notificationPeriod != null) ? (this.notificationPeriod + ":" + this.mnemonic) : this.mnemonic;
    }
}
