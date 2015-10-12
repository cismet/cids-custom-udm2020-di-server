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

import java.sql.Date;

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
public class Notification {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private int id = -1;

    @JacksonXmlProperty(isAttribute = true)
    private String mnemonic;

    @JacksonXmlProperty(isAttribute = true)
    private String name;

    @JsonProperty("notificationstartdate")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "notificationstartdate"
    )
    private Date notificationStartDate;

    @JsonProperty("notificationenddate")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "notificationenddate"
    )
    private Date notificationEndDate;

    @JsonProperty("reportingstartdate")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "reportingstartdate"
    )
    private Date reportingStartDate;

    @JsonProperty("reportingenddate")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "reportingenddate"
    )
    private Date reportingEndDate;
}
