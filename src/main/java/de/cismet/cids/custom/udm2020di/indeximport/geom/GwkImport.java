/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport.geom;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DOCUMENT ME!
 *
 * @author   Pascal Dihé
 * @version  $Revision$, $Date$
 */
public class GwkImport extends GeomImport {

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger LOGGER = Logger.getLogger(GwkImport.class);

    //~ Instance fields --------------------------------------------------------

    protected final PreparedStatement insertGwk;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BundeslaenderImport object.
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public GwkImport() throws Exception {
        super(GwkImport.class.getResourceAsStream("gwk.properties"));

        final String insertGwkTlp = IOUtils.toString(this.getClass().getResourceAsStream(
                    "/de/cismet/cids/custom/udm2020di/indeximport/geom/insert-gwk.prs.sql"),
                "UTF-8");

        insertGwk = this.targetConnection.prepareStatement(insertGwkTlp);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    @Override
    public int doImport() throws Exception {
        int i = 0;

        try {
            final BufferedReader br = new BufferedReader(
                    new InputStreamReader(this.getClass().getResourceAsStream("gwk_at.wkt"), "UTF8"));

            String line;
            while ((line = br.readLine()) != null) {
                final ResultSet rs = psv.executeQuery();
                rs.next();
                final int id = rs.getInt(1);

                // insert geom
                final String[] split = line.split(";");
                final String name = split[1];

                psg.setInt(1, id);
                psg.setClob(2, new StringReader(split[0]));
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("inserting new GKW geometry");
                }
                psg.executeUpdate();

                // insert named area
                insertGwk.setString(1, name);
                insertGwk.setInt(2, id);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("inserting new GWK NAMED_AREA '" + name + "' with GEOM.id " + id);
                }
                insertGwk.executeUpdate();

                i++;
                if (log.isDebugEnabled()) {
                    log.info("inserted GWK '" + name + "'");
                }
            }
            this.targetConnection.commit();
        } catch (Exception ex) {
            log.error("rolling back GWK-Import due to exception: " + ex.getMessage(), ex);

            try {
                this.targetConnection.rollback();
                this.targetConnection.close();
                System.exit(1);
            } catch (SQLException sx) {
                log.error("could not rollback or close target connection", sx);
                System.exit(1);
            }
        }

        if (log.isDebugEnabled()) {
            // clean up
            log.debug("closing connections ....");
        }

        this.psg.close();
        this.insertGwk.close();
        this.psv.close();

        return i;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  the command line arguments
     */
    public static void main(final String[] args) {
        try {
            GwkImport.LOGGER.info("Starting GWK Import");

            final long startTime = System.currentTimeMillis();
            final GwkImport bundeslaenderImport = new GwkImport();
            final int i = bundeslaenderImport.doImport();

            GwkImport.LOGGER.info(i + " GWK successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
        } catch (Exception ex) {
            GwkImport.LOGGER.error("could not perform GWK Import: "
                        + ex.getMessage(), ex);
        }
    }
}
