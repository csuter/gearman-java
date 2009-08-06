/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import java.io.IOException;

import junit.framework.Assert;

import org.gearman.common.GearmanPacketType;
import org.gearman.util.ByteUtils;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.gearman.worker.GearmanFunction;
import org.gearman.worker.GearmanFunctionFactory;
import org.gearman.common.GearmanPacket;
import org.gearman.worker.AbstractGearmanFunction;
import org.gearman.worker.GearmanWorker;

import org.gearman.worker.GearmanWorkerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class GearmanClientJobExecTest {

    GearmanClientImpl gc = null;
    GearmanWorker worker = null;
    Thread workerThread = null;
    WorkerRunnable runner = null;
    Thread wt = null;
    GearmanFunctionFactory lrff = null;

    class incrementalReverseFunctionFactory implements GearmanFunctionFactory {

        public String getFunctionName() {
            return incrementalReverseFunction.class.getCanonicalName();
        }

        public GearmanFunction getFunction() {
            return new incrementalReverseFunction();
        }

    }

    class incrementalReverseFunction extends AbstractGearmanFunction {

        public GearmanJobResult executeFunction() throws Exception {
            StringBuffer sb = null;
            if (data instanceof byte []) {
                sb = new StringBuffer(ByteUtils.fromUTF8Bytes((byte [])data));
            } else {
                sb = new StringBuffer(data.toString());
            }
            sb = sb.reverse();

            for (int i = 0; i < sb.length(); i++) {
                sendData(sb.substring(i, i + 1).getBytes());
            }
            return new GearmanJobResultImpl(jobHandle, true, new byte[0],
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

    class newReverseFunction extends AbstractGearmanFunction {

        @Override
        public GearmanJobResult executeFunction() throws Exception {
            StringBuffer sb = null;
            byte [] results = null;
            if (data instanceof byte []) {
                sb = new StringBuffer(ByteUtils.fromUTF8Bytes((byte [])data));
            } else {
                sb = new StringBuffer(data.toString());
            }
            results = sb.reverse().toString().getBytes();
            return new GearmanJobResultImpl(jobHandle, true, results,
                    new byte[0], new byte[0], 0, 0);
        }
    }

    class longRunningFunctionFactory implements GearmanFunctionFactory {
        longRunningFunction lrf = null;

        public String getFunctionName() {
            return longRunningFunction.class.getCanonicalName();
        }

        public synchronized GearmanFunction getFunction() {
            if (lrf == null) {
                lrf = new longRunningFunction();
            }
            return lrf;
        }
    }

    class longRunningFunction extends AbstractGearmanFunction {

        boolean keepRunning = true;

        public GearmanJobResult executeFunction() throws Exception {
            while (keepRunning) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                }
            }
            return new GearmanJobResultImpl(jobHandle, true, new byte[0],
                    new byte[0], new byte[0], 0, 0);
        }

        public void isDone() {
            keepRunning = false;
        }

        public boolean isRunning() {
            return keepRunning;
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

    class IncrementalListener implements GearmanIOEventListener {

        private StringBuffer sb = new StringBuffer();
        public void handleGearmanIOEvent(GearmanPacket event) 
                throws IllegalArgumentException {
            if (!event.getPacketType().equals(GearmanPacketType.WORK_DATA)) {
                return;
            }
            sb.append(ByteUtils.fromUTF8Bytes((event.getDataComponentValue(
                        GearmanPacket.DataComponentName.DATA))));
        }
        public String getResults() {
            return sb.toString();
        }

    }

    @Before
    public void initTest() throws IOException {
        gc = new GearmanClientImpl();
        worker = new GearmanWorkerImpl();
        gc.addJobServer(new GearmanNIOJobServerConnection("localhost"));

        lrff = new longRunningFunctionFactory();
        worker.registerFunctionFactory(lrff);
        worker.registerFunctionFactory(new newReverseFunctionFactory());
        worker.registerFunctionFactory(new incrementalReverseFunctionFactory());

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
                    newReverseFunction.class.getCanonicalName(),
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
                    incrementalReverseFunction.class.getCanonicalName(),
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
                    newReverseFunction.class.getCanonicalName(),
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
        longRunningFunction lrf = (longRunningFunction) lrff.getFunction();
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
            lrf.keepRunning = true;

        }
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
