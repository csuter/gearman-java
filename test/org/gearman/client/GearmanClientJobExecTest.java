/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import java.io.IOException;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.Assert;

import org.gearman.common.Constants;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.gearman.tests.functions.FailedFunction;
import org.gearman.tests.functions.FailedFunctionFactory;
import org.gearman.tests.functions.IncrementalReverseFunction;
import org.gearman.tests.functions.IncrementalReverseFunctionFactory;
import org.gearman.tests.functions.LongRunningFunction;
import org.gearman.tests.functions.LongRunningFunctionFactory;
import org.gearman.tests.functions.ReverseFunction;
import org.gearman.tests.functions.ReverseFunctionFactory;
import org.gearman.tests.util.IncrementalListener;
import org.gearman.tests.util.WorkerRunnable;
import org.gearman.util.ByteUtils;
import org.gearman.worker.GearmanWorker;
import org.gearman.worker.GearmanWorkerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GearmanClientJobExecTest {

    GearmanClientImpl gc = null;
    GearmanWorker worker = null;
    Thread workerThread = null;
    WorkerRunnable runner = null;
    Thread wt = null;
    LongRunningFunctionFactory lrff = null;

    @Before
    public void initTest() throws IOException {
        gc = new GearmanClientImpl();
        worker = new GearmanWorkerImpl();
        gc.addJobServer(new GearmanNIOJobServerConnection("localhost"));

        lrff = new LongRunningFunctionFactory();
        worker.registerFunctionFactory(lrff);
        worker.registerFunctionFactory(new ReverseFunctionFactory());
        worker.registerFunctionFactory(new IncrementalReverseFunctionFactory());
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
    public void simpleAttachedJob()
            throws IOException, InterruptedException, ExecutionException {
        for (int i = 0; i < 100; i++) {
            StringBuffer text = generateData(8193, "Hello World");
            GearmanJob job = GearmanJobImpl.createJob(
                    ReverseFunction.class.getCanonicalName(),
                    ByteUtils.toAsciiBytes(text.toString()),null);
            GearmanJobResult result = gc.submit(job).get();
            Assert.assertTrue(result.jobSucceeded());
            Assert.assertTrue("Client reports active jobs even though all " +
                    "jobs have completed",gc.getNumberofActiveJobs() == 0);
            String resultString = ByteUtils.fromAsciiBytes(result.getResults());
            Assert.assertTrue(resultString.equals(text.reverse().toString()));
        }
    }

    @Test
    public void incrementalAttachedJob()
            throws IOException, InterruptedException, ExecutionException {
            StringBuffer text = generateData(8193, "Hello World");
            GearmanJob job = GearmanJobImpl.createJob(
                    IncrementalReverseFunction.class.getCanonicalName(),
                    ByteUtils.toAsciiBytes(text.toString()), null);
            IncrementalListener il = new IncrementalListener();
            job.registerEventListener(il);
            gc.submit(job);
            GearmanJobResult result = job.get();
            Assert.assertTrue(result.jobSucceeded());
            Assert.assertTrue("Client reports active jobs even though all " +
                    "jobs have completed", gc.getNumberofActiveJobs() == 0);
            String resultString = il.getResults();
            Assert.assertTrue(resultString.equals(text.reverse().toString()));
    }

    @Test
    public void simpleAttachedJobBulk() throws IOException,
            InterruptedException, TimeoutException, ExecutionException {
        int num = 1000;
        int numCompleted = 0;
        StringBuffer text = generateData(8193, "Hello World");
        ArrayList <Future> futures = new ArrayList<Future>();
        for (int i = 0; i < num; i++) {
            GearmanJob job = GearmanJobImpl.createJob(
                    ReverseFunction.class.getCanonicalName(),
                    ByteUtils.toAsciiBytes(text.toString()),null);
            futures.add(gc.submit(job));
            Assert.assertTrue(job.getHandle() != null);
            
        }
        String rtext = text.reverse().toString();
        for (Future<GearmanJobResult> curJob : futures) {
            GearmanJobResult curRes = curJob.get(20, TimeUnit.SECONDS);
            numCompleted++;
            Assert.assertTrue(curRes.jobSucceeded());
            Assert.assertTrue("Client reports active jobs even though all " +
                    "jobs have completed",
                    gc.getNumberofActiveJobs() <= num - numCompleted);
            String resultString = ByteUtils.fromAsciiBytes(curRes.getResults());
            Assert.assertTrue(resultString.equals(rtext));
        }
    }

    @Test
    public void simpleDetachedTest()
            throws IOException, InterruptedException, ExecutionException,
            TimeoutException {
        GearmanJobStatus status = null;
        LongRunningFunction lrf = (LongRunningFunction) lrff.getFunction();
        for (int x = 0; x < 10; x++) {
            GearmanJobImpl job = (GearmanJobImpl)
                    GearmanJobImpl.createBackgroundJob(lrff.getFunctionName(),
                    null, null);
            gc.submit(job);
            boolean hasHitRunning = false;
            for (int i = 0; i < 5; i++) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {
                }
                status = gc.getJobStatus(job);
                Assert.assertTrue("updateJobStatus reports submitted job as" +
                        " unknown on " + x + "/" + i + " iteration of test",
                        status.isKnown());
                if (status.isRunning()) {
                    hasHitRunning = true;
                }
                if (hasHitRunning) {
                    Assert.assertTrue("updateJobStatus reports reports " +
                            "unexpected running state", status.isRunning());
                }

            }
            lrf.isDone();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
            status = gc.getJobStatus(job);
            Assert.assertTrue("status never reported the job as running",
                    hasHitRunning);
            Assert.assertFalse("updateJobStatus reports completed job as known",
                    status.isKnown());
            Assert.assertFalse("updateJobStatus reports completed job as " +
                    "running", status.isRunning());
            lrf.reset();
        }
    }

    @Test
    public void bigPayloadTest() throws ExecutionException, InterruptedException {
        String id = "null";
        int messageOverheadSize = Constants.GEARMAN_PACKET_HEADER_SIZE +
                ReverseFunction.class.getCanonicalName().getBytes().length +
                id.getBytes().length + 2;
        int [] payLoadSizes = {
            Constants.GEARMAN_PACKET_HEADER_SIZE - messageOverheadSize,
            Constants.GEARMAN_PACKET_HEADER_SIZE - messageOverheadSize + 1,
            Constants.GEARMAN_PACKET_HEADER_SIZE - messageOverheadSize - 1,
            (int)((Constants.GEARMAN_PACKET_HEADER_SIZE - messageOverheadSize) * 1.5),
            (Constants.GEARMAN_PACKET_HEADER_SIZE * 2) - messageOverheadSize,
            1024 * 1000 * 3, //3MB
        };
        for (int curSize : payLoadSizes) {
            StringBuffer text = generateData(curSize,"Hello World");
            GearmanJob job = GearmanJobImpl.createJob(
                ReverseFunction.class.getCanonicalName(),
                ByteUtils.toUTF8Bytes(text.toString()), id);
            GearmanJobResult jr = gc.submit(job).get();
            Assert.assertTrue("bigPayloadTest (size =" + curSize +
                    ") did not succeed.",jr.jobSucceeded());
            String resultString = ByteUtils.fromUTF8Bytes(jr.getResults());
            Assert.assertTrue("bigPayloadTest (size =" + curSize +
                    ") returned unexpected results.",
                    resultString.equals(text.reverse().toString()));
        }
    }

    @Test
    public void bug511489() throws ExecutionException, InterruptedException {
        GearmanJob job = GearmanJobImpl.createJob(
            FailedFunction.class.getCanonicalName(), null,null);
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

    private StringBuffer generateData(int size, String pattern) {
        StringBuffer b = new StringBuffer();
        int leftToWrite = size;
        while (leftToWrite > 0) {
            int amount = pattern.length() > leftToWrite ?
                leftToWrite : pattern.length();
            b.append(pattern.substring(0, amount));
            leftToWrite -= amount;
        }
        return b;
    }
}
