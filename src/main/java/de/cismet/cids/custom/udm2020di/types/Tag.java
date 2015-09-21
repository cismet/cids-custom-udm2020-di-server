/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@JacksonXmlRootElement
public class Tag {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty
    private long id;

    @JacksonXmlProperty
    private String key;

    @JacksonXmlProperty
    private String description;

    @JacksonXmlProperty(localName = "taggroupid")
    @JsonProperty("taggroupid")
    private long taggroupId;

    @JacksonXmlProperty(localName = "taggroupkey")
    @JsonProperty("taggroupkey")
    private String taggroupKey;

    @JacksonXmlProperty
    private String name;

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public long getId() {
        return id;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  id  DOCUMENT ME!
     */
    public void setId(final long id) {
        this.id = id;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getKey() {
        return key;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  key  DOCUMENT ME!
     */
    public void setKey(final String key) {
        this.key = key;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getDescription() {
        return description;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  description  DOCUMENT ME!
     */
    public void setDescription(final String description) {
        this.description = description;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public long getTaggroupId() {
        return taggroupId;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  taggroupId  DOCUMENT ME!
     */
    public void setTaggroup(final long taggroupId) {
        this.taggroupId = taggroupId;
    }

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     */
    public String getTaggroupKey() {
        return taggroupKey;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  taggroupKey  DOCUMENT ME!
     */
    public void setTaggroupKey(final String taggroupKey) {
        this.taggroupKey = taggroupKey;
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
}
