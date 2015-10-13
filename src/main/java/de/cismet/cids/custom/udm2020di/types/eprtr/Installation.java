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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import de.cismet.cids.custom.udm2020di.serializers.FlexibleFloatDeserializer;
import de.cismet.cids.custom.udm2020di.types.AggregationValue;
import de.cismet.cids.custom.udm2020di.types.Parameter;
import de.cismet.cids.custom.udm2020di.types.Tag;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@XmlRootElement
@NoArgsConstructor
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Installation {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty(isAttribute = true)
    private long id;

    @JacksonXmlProperty(isAttribute = true)
    private String name;

    @JsonProperty("erasid")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "erasid"
    )
    private long erasId;

    @JsonProperty("naceclass")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "naceclass"
    )
    private String naceClass;

    @JsonProperty("obligatedparty")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "obligatedparty"
    )
    private String obligatedParty;

    @JsonProperty("rivercatchment")
    @JacksonXmlProperty(
        isAttribute = true,
        localName = "rivercatchment"
    )
    private String riverCatchment;

    @JacksonXmlProperty(isAttribute = true)
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float longitude;

    @JacksonXmlProperty(isAttribute = true)
    @JsonDeserialize(
        using = FlexibleFloatDeserializer.class,
        as = float.class
    )
    private float latitude;

    @JacksonXmlProperty
    @Setter(AccessLevel.NONE)
    private List<Tag> tags;

    @JacksonXmlProperty
    @Setter(AccessLevel.NONE)
    private List<Notification> notifications;

    @JacksonXmlProperty(localName = "aggregationvalues")
    @JsonProperty("aggregationvalues")
    @Setter(AccessLevel.NONE)
    private List<AggregationValue> aggregationValues;

    @JacksonXmlProperty
    @Setter(AccessLevel.NONE)
    private List<Activity> activities;

    @JacksonXmlProperty
    private List<Address> addresses;

    @JacksonXmlProperty(localName = "releaseparameters")
    @JsonProperty("releaseparameters")
    @Setter(AccessLevel.NONE)
    private List<Parameter> releaseParameters;

    //~ Methods ----------------------------------------------------------------

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
     * @param  notifications  DOCUMENT ME!
     */
    public void setNotifications(final List<Notification> notifications) {
        this.notifications = notifications;
        if ((this.notifications != null) && !this.notifications.isEmpty()) {
            Collections.sort(this.notifications);
        }
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
     * DOCUMENT ME!
     *
     * @param  activities  DOCUMENT ME!
     */
    public void setActivities(final List<Activity> activities) {
        this.activities = activities;
        if ((this.activities != null) && !this.activities.isEmpty()) {
            Collections.sort(this.activities);
        }
    }

    /**
     * DOCUMENT ME!
     *
     * @param  releaseParameters  DOCUMENT ME!
     */
    public void setReleaseParameters(final List<Parameter> releaseParameters) {
        this.releaseParameters = releaseParameters;
        if ((this.releaseParameters != null) && !this.releaseParameters.isEmpty()) {
            Collections.sort(this.releaseParameters);
        }
    }
}
