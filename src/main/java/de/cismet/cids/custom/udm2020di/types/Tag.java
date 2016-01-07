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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Objects;

import de.cismet.cids.dynamics.CidsBean;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement
public class Tag implements Cloneable, Comparable<Tag> {

    //~ Instance fields --------------------------------------------------------

    @JacksonXmlProperty private long id;

    @JacksonXmlProperty private String key;

    @JacksonXmlProperty private String description;

    @JacksonXmlProperty(localName = "taggroupid")
    @JsonProperty("taggroupid")
    private long taggroupId;

    @JacksonXmlProperty(localName = "taggroupkey")
    @JsonProperty("taggroupkey")
    private String taggroupKey;

    @JacksonXmlProperty private String name;

    @JacksonXmlProperty private boolean selected = false;

    //~ Constructors -----------------------------------------------------------

    /**
     * Copy-Constructor.
     *
     * @param  tag  DOCUMENT ME!
     */
    public Tag(final Tag tag) {
        this();
        this.id = tag.id;
        this.key = tag.key;
        this.name = tag.name;
        this.description = tag.description;
        this.selected = tag.selected;
        this.taggroupId = tag.taggroupId;
        this.taggroupKey = tag.taggroupKey;
    }

    /**
     * Creates a new Tag object.
     *
     * @param  cidsBean  DOCUMENT ME!
     */
    public Tag(final CidsBean cidsBean) {
        this.id = (cidsBean.getProperty("id") != null) ? (int)cidsBean.getProperty("id") : -1;

        this.key = (cidsBean.getProperty("key") != null) ? cidsBean.getProperty("key").toString() : null;

        this.name = (cidsBean.getProperty("name") != null) ? cidsBean.getProperty("name").toString() : null;

        this.description = (cidsBean.getProperty("description") != null)
            ? cidsBean.getProperty("description").toString() : null;

        this.taggroupId = (cidsBean.getProperty("taggroup.id") != null) ? (int)cidsBean.getProperty("taggroup.id")
                                                                        : -1L;

        this.taggroupKey = (cidsBean.getProperty("taggroup_key") != null)
            ? cidsBean.getProperty("taggroup_key").toString() : null;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public int compareTo(final Tag tag) {
        return this.getName().compareTo(tag.getName());
    }

    @Override
    public String toString() {
        return this.getName();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!this.getClass().isAssignableFrom(obj.getClass())) {
            return false;
        }
        final Tag other = (Tag)obj;

        if (!Objects.equals(this.id, other.id)) {
            return false;
        }

        if (!Objects.equals(this.key, other.key)) {
            return false;
        }

        if (!Objects.equals(this.taggroupKey, other.taggroupKey)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = (29 * hash) + Objects.hashCode(this.id);
        hash = (29 * hash) + Objects.hashCode(this.key);
        hash = (29 * hash) + Objects.hashCode(this.taggroupKey);
        return hash;
    }

    @Override
    public Tag clone() throws CloneNotSupportedException {
        return new Tag(this);
    }
}
