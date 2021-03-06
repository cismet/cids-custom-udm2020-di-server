/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport.geom;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DOCUMENT ME!
 *
 * @author   martin.scholl@cismet.de
 * @version  $Revision$, $Date$
 */
public class BundeslaenderImport extends GeomImport {

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BundeslaenderImport object.
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public BundeslaenderImport() throws Exception {
        super(BundeslaenderImport.class.getResourceAsStream("bundeslaender.properties"));
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
                    new InputStreamReader(this.getClass().getResourceAsStream("bundeslaender.wkt"), "UTF8"));

            String line;
            while ((line = br.readLine()) != null) {
                final ResultSet rs = psv.executeQuery();
                rs.next();
                final int id = rs.getInt(1);

                // insert geom
                final String[] split = line.split(";");
                psg.setInt(1, id);
                psg.setClob(2, new StringReader(split[2]));
                psg.executeUpdate();

                // insert named area
                insertNamedArea.setString(1, split[0]);
                insertNamedArea.setString(2, split[1]);
                insertNamedArea.setInt(3, id);
                insertNamedArea.executeUpdate();
                i++;
                if (log.isDebugEnabled()) {
                    log.debug("inserted Bundesland '" + split[0] + "' of type '"
                                + split[1] + "'");
                }
            }
            this.targetConnection.commit();
        } catch (Exception ex) {
            log.error("rolling back Bundesländer-Import due to exception: " + ex.getMessage(), ex);

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
        this.insertNamedArea.close();
        this.psv.close();

        return i;
    }

    /**
     * DOCUMENT ME!
     *
     * @param  args  the command line arguments
     */
    public static void main(final String[] args) {
        final Logger logger = Logger.getLogger(BundeslaenderImport.class);

        try {
            logger.info("Starting Bundesländer  Import");

            final long startTime = System.currentTimeMillis();
            final BundeslaenderImport bundeslaenderImport = new BundeslaenderImport();
            final int i = bundeslaenderImport.doImport();

            logger.info(i + " Bundesländer successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
        } catch (Exception ex) {
            logger.error("could not perform Bundeslaender Import: "
                        + ex.getMessage(), ex);
        }
    }
}
