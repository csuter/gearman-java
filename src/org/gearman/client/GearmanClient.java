/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.gearman.common.GearmanJobServerConnection;
import org.gearman.common.GearmanPacket;

/**
 * This interface through which users of the Gearman Java Libaray will use to
 * submit jobs for execution by the Gearman System and to track the progess of
 * those jobs.
 *
 * <p>
 * Users of a GearmanClient will first instantiate a <tt>GearmanClient</tt>
 * instance. After the instance has been created, the user will need to
 * register the set of {@link GearmanJobServerConnection} connections which are
 * to be availble to client for job submission.
 *
 * <p>
 * Once the <tt>GearmanClient</tt> has been configured with the appropriate
 * set of {@link GearmanJobServerConnection}, the user may begin using the
 * <tt>GearmanClient</tt> to submit jobs for execution. This is accomplished by
 * passing a {@link GearmanJob} to either the
 * {@link GearmanClient#submit(java.util.concurrent.Callable) } or the
 * {@link GearmanClient#invokeAll(java.util.Collection) } methods. Both of
 * these methods return a {@link Future} object which can be used to track
 * the state of that job.
 *
 * <p>
 * A <tt>GearmanClient</tt> can be shut down, which will cause it
 * to stop accepting new tasks.  After being shut down, the client
 * will eventually terminate, at which point no tasks are actively
 * executing, no tasks are awaiting execution, and no new tasks can be
 * submitted
 *
 *  <h3>Usage Example</h3>
 *
 * Here is an example of how to use a <tt>GearmanClient</tt> to submit and
 * monitor the execution of a {@link GearmanJob} (Note, this example does not
 * show the creation of the GearmanClient instance nor the creation of the
 * GearmanJob or Connection as these details are implementation specific):
 *
 * <pre>
 *     org.gearman.common.GearmanJobServerConnection conn;
 *     org.gearman.client.GearmanClient client;
 *     org.gearman.client.GearmanJob job;
 *
 *     // Here you would instantiate the client and job
 *
 *     client.addJobServer(conn);
 *
 *     Future<GearmanJobResult> jobFuture = client.submit(job);
 *
 *     GearmanJobResult result = jobFuture.get();
 *     ...
 *
 * </pre>
 *
 */
public interface GearmanClient extends ExecutorService {

    /**
     * Register a new Gearman Job Server with the client.
     *
     * @param conn The connection to the Gearman Job Server.
     *
     * @throws IllegalArgumentException If an invalid connection has been
     * specified.
     * @throws IllegalStateException If the client has already been stopped.
     */
    void addJobServer(GearmanJobServerConnection conn)
            throws IllegalArgumentException, IllegalStateException;

    /**
     * Retrieve the list of Gearman Job Servers connections that have been
     * registered with this client.
     *
     * @return the list of registered Gearman Job Servers.
     *
     * @throws IllegalStateException If the client has already been stopped.
     */
    List<GearmanJobServerConnection> getSetOfJobServers()
            throws IllegalStateException;

    /**
     * Unregisters a Gearman Job Server Connection from use by this client.
     *
     * @param conn The connection to the Gearman Job Server to be unregistered.
     *
     * @throws IllegalArgumentException If an invalid connection has been
     * specified.
     * @throws IllegalStateException If the client has already been stopped or
     *         the connection is currently processing one or more requests.
     */
    void removeJobServer(GearmanJobServerConnection conn)
            throws IllegalArgumentException,
            IllegalStateException;

    /**
     * As {@link GearmanJob} are submitted and executed, they will receive
     * notifications from the Gearman Job Server as certain packets are received
     * (such as JOB_COMPLETE or WORK_DATA). Users may want to process these
     * packets,for example if jobs are sending back intermediate data, a user
     * may wish to receive that data and act upon it. This method returns to
     * the user all events which have occured for any job submitted by the
     * client.
     *
     * @return A collection of {@link GearmanPacket} that have been received.
     *
     * @throws IllegalStateException If the client has already been stopped.
     */
    Collection<GearmanPacket> selectUpdatedJobEvents()
            throws IllegalStateException;

    /**
     * Sends a WORK_STATUS request to the Gearman Job Server running the
     * {@link GearmanJob} and then returns {@link GearmanJobStatus} representing
     * the current status of the job.
     *
     * @param job The job for which status request is being made.
     * @return The status of the specified job.
     * @throws IOException
     * @throws IllegalStateException If the client has already been stopped.
     */
    GearmanJobStatus getJobStatus(GearmanJob job) throws IOException,
            IllegalStateException;
}
