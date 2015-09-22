/***************************************************
*
* cismet GmbH, Saarbruecken, Germany
*
*              ... and it just works.
*
****************************************************/
package de.cismet.cids.custom.udm2020di.indeximport.geom;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import de.cismet.cids.custom.udm2020di.indeximport.OracleImport;

/**
 * DOCUMENT ME!
 *
 * @author   martin.scholl@cismet.de
 * @version  $Revision$, $Date$
 */
public class BundeslaenderImport extends OracleImport {

    //~ Instance fields --------------------------------------------------------

    protected final PreparedStatement psv;
    protected final PreparedStatement psg;
    protected final PreparedStatement psn;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new BundeslaenderImport object.
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public BundeslaenderImport() throws Exception {
        this(BundeslaenderImport.class.getResourceAsStream("bundeslaender.properties"));
    }

    /**
     * Creates a new BundeslaenderImport object.
     *
     * @param   propertiesFile  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
    public BundeslaenderImport(final InputStream propertiesFile) throws Exception {
        super(propertiesFile);

        psv = this.targetConnection.prepareStatement("select geom_seq.nextval from dual");
        psg = this.targetConnection.prepareStatement(
                "insert into geom (id, geo_field) values (?, sdo_geometry(?, 4326))");
        psn = this.targetConnection.prepareStatement("insert into named_area (name, type, area) values (?, ?, ?)");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * DOCUMENT ME!
     *
     * @return  DOCUMENT ME!
     *
     * @throws  Exception  DOCUMENT ME!
     */
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
                psn.setString(1, split[0]);
                psn.setString(2, split[1]);
                psn.setInt(3, id);
                psn.executeUpdate();
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
        this.psn.close();
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
            log.info("Starting Bundesländer  Import");

            final long startTime = System.currentTimeMillis();
            final BundeslaenderImport bundeslaenderImport = new BundeslaenderImport();
            final int i = bundeslaenderImport.doImport();

            log.info(i + " Bundesländer successfully imported in "
                        + ((System.currentTimeMillis() - startTime) / 1000 / 60) + "m");
        } catch (Exception ex) {
            BundeslaenderImport.log.error("could not perform Bundeslaender Import: "
                        + ex.getMessage(), ex);
        }
    }
}
