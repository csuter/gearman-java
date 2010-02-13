/*
 * Copyright (C) 2010 by Eric Lambert <eric.d.lambert@gmail.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import junit.framework.Assert;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.gearman.tests.functions.FailedFunction;
import org.gearman.tests.functions.FailedFunctionFactory;
import org.gearman.tests.util.WorkerRunnable;
import org.gearman.worker.GearmanWorker;
import org.gearman.worker.GearmanWorkerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Bug511489Test {

    GearmanClientImpl gc = null;
    GearmanWorker worker = null;
    Thread workerThread = null;
    WorkerRunnable runner = null;
    Thread wt = null;

    @Before
    public void initTest() throws IOException {
        gc = new GearmanClientImpl();
        worker = new GearmanWorkerImpl();
        gc.addJobServer(new GearmanNIOJobServerConnection("localhost"));
        worker.registerFunctionFactory(new FailedFunctionFactory());

        //create a worker for each of the job servers
        worker.addServer(new GearmanNIOJobServerConnection("localhost"));

        runner = new WorkerRunnable(worker);
        wt = startWorkerThread(runner, "workerThread");
    }

    @After
    public void shutdownTest() throws IOException {
        gc.shutdownNow();
        worker.stop();
        runner.stop();

    }

    @Test
    public void bug511489() throws ExecutionException, InterruptedException {
        GearmanJob job = GearmanJobImpl.createJob(
                FailedFunction.class.getCanonicalName(), null, null);
        GearmanJobResult result = gc.submit(job).get();
        Assert.assertFalse(result.jobSucceeded());
    }

    private Thread startWorkerThread(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException ioe) {
        }
        return t;
    }
}
