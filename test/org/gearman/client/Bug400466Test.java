/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import junit.framework.Assert;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.gearman.util.ByteUtils;
import org.gearman.worker.AbstractGearmanFunction;
import org.gearman.worker.GearmanFunction;
import org.gearman.worker.GearmanFunctionFactory;
import org.gearman.worker.GearmanWorker;
import org.gearman.worker.GearmanWorkerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Bug400466Test {

    GearmanClientImpl gc = null;
    GearmanWorker worker = null;
    Thread workerThread = null;
    WorkerRunnable runner = null;
    Thread wt = null;
    byte[] data = new byte[8193];
    Runtime rt = null;

    class newReverseFunction extends AbstractGearmanFunction {

        @Override
        public GearmanJobResult executeFunction() throws Exception {
            StringBuffer sb = null;
            byte[] results = null;
            if (data instanceof byte[]) {
                sb = new StringBuffer(ByteUtils.fromUTF8Bytes((byte[]) data));
            } else {
                sb = new StringBuffer(data.toString());
            }
            results = sb.reverse().toString().getBytes();
            return new GearmanJobResultImpl(jobHandle, true, results,
                    new byte[0], new byte[0], 0, 0);
        }
    }

    class newReverseFunctionFactory implements GearmanFunctionFactory {

        public String getFunctionName() {
            return newReverseFunction.class.getCanonicalName();
        }

        public GearmanFunction getFunction() {
            return new newReverseFunction();
        }
    }

    class WorkerRunnable implements Runnable {

        GearmanWorker myWorker = null;
        boolean isRunning = true;

        public WorkerRunnable(GearmanWorker w) {
            myWorker = w;
        }

        public void run() {
            while (isRunning) {
                myWorker.work();
            }
        }

        public void stop() {
            isRunning = false;
        }
    }

    @Before
    public void initTest() throws IOException {
        for (int i = 0; i < data.length; i++) {
            data[i] = '0';
        }
        rt = Runtime.getRuntime();
        gc = new GearmanClientImpl();
        worker = new GearmanWorkerImpl();
        gc.addJobServer(new GearmanNIOJobServerConnection("localhost"));

        //create a worker for each of the job servers
        worker.addServer(new GearmanNIOJobServerConnection("localhost"));
        worker.registerFunctionFactory(new newReverseFunctionFactory());
        runner = new WorkerRunnable(worker);
        wt = new Thread(runner, "workerThread");
        wt.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException ioe) {
        }
    }

    @After
    public void shutdownTest() throws IOException {
        if (gc != null) {
            gc.shutdownNow();
        }
        if (worker != null) {
            worker.stop();
        }
        if (runner != null) {
            runner.stop();
        }
    }

    @Test
    public void test400466()
            throws IOException, InterruptedException, ExecutionException {
        long heapSizeCeiling = 0;
        long memUsed = 0;
        for (int x = 1; x <= 1000; x++) {
            GearmanJob job = GearmanJobImpl.createJob(
                    newReverseFunction.class.getCanonicalName(),
                    data, null);
            gc.submit(job);
            if (x % 100 == 0) {
                memUsed = rt.totalMemory() - rt.freeMemory();
                if (heapSizeCeiling == 0) {
                    heapSizeCeiling = memUsed * 4;
                } else {
                    Assert.assertTrue("ceiling = " + heapSizeCeiling +
                            " used = " + memUsed, memUsed < heapSizeCeiling);
                }
            }
        }
        //Submit once last job and wait on it so as to clear the server q
        GearmanJob job = GearmanJobImpl.createJob(
                newReverseFunction.class.getCanonicalName(),
                data, null);
        gc.submit(job).get();
    }
}
