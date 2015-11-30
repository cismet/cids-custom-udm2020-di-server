/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport.geom;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;

import java.sql.PreparedStatement;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public abstract class GeomImport extends OracleImport {

    //~ Instance fields --------------------------------------------------------

    protected final PreparedStatement psv;
    protected final PreparedStatement psg;
    protected final PreparedStatement insertNamedArea;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BundeslaenderImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public GeomImport(final InputStream propertiesFile) throws Exception {
        super(propertiesFile);

        final String psvTlp = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/geom/psv.prs.sql"),
                "UTF-8");
        psv = this.targetConnection.prepareStatement(psvTlp);

        final String psgTlp = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/geom/psg.prs.sql"),
                "UTF-8");
        psg = this.targetConnection.prepareStatement(psgTlp);

        final String insertNamedAreaTlp = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/geom/insert-named-area.prs.sql"),
                "UTF-8");

        insertNamedArea = this.targetConnection.prepareStatement(insertNamedAreaTlp);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public abstract int doImport() throws Exception;
}
