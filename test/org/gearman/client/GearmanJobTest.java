/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the
 * GNU Lesser General Public License (LGPL) version 2.1.
 * See the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import junit.framework.Assert;
import org.gearman.common.Constants;
import org.gearman.common.GearmanPacket;
import org.gearman.common.GearmanPacketMagic;
import org.gearman.common.GearmanPacketType;
import org.gearman.client.GearmanJob.JobPriority;
import org.gearman.util.ByteUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.gearman.worker.GearmanFunction;
import org.gearman.client.GearmanJob;
import org.gearman.client.GearmanJobResult;

//TODO finish me
public class GearmanJobTest {
    public static void main (String [] args) {
        GearmanJobTest me = new GearmanJobTest();
        try {
        me.basicJobTest();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
//    class reverseFunction implements GearmanFunction {
//        byte [] bytes;
//        public void setData(Object data) {
//            bytes = (byte [])data;
//        }
//
//        public GearmanJobResult call () {
//            String s = ByteUtils.fromUTF8Bytes(bytes);
//            StringBuffer sb = new StringBuffer(s);
//            byte [] resBytes = sb.reverse().toString().getBytes();
//            GearmanJobResultImpl res = new GearmanJobResultImpl("".getBytes(), true, resBytes, null, null, 0, 0);
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException ie) {}
//            return res;
//        }
//    }

    @Test
    public void basicJobTest () throws Exception {
//        StringBuffer text = new StringBuffer("Hello World");
//        GearmanJob job = GearmanJobImpl.createJob("reverse", text.toString().getBytes(), null);
//        GearmanFunction myFunction = new reverseFunction();
//        job.registerFunction(myFunction);
//        ExecutorService es = new ThreadPoolExecutor(1,2,1000,TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
//        GearmanJobResult f = es.submit(job).get();
//        f.getResults();
//        String resultString = ByteUtils.fromAsciiBytes(f.getResults());
//        Assert.assertTrue(resultString.equals(text.reverse().toString()));
//        System.out.println("THE RESULTS ARE " + resultString);

    }

    
    public void ctorTest() throws IOException {
        GearmanJobImpl job = null;
//        String nullFun = null;
//        String emptyFun = "";
//        String validFun = "aFun";
//        byte[] nullData = null;
//        byte[] nonEmptyData = new byte[1];
//        String id = "anID";
//
//        job = new GearmanJobImpl(validFun, nonEmptyData, false, JobPriority.LOW,
//                id);
//        validateJob(job, validFun, nonEmptyData, JobPriority.LOW, id);
//
//        job = new GearmanJobImpl(validFun, nonEmptyData, false, JobPriority.LOW);
//        validateJob(job, validFun, nonEmptyData, JobPriority.LOW, null);
//
//        job = new GearmanJobImpl(validFun, nonEmptyData, false);
//        validateJob(job, validFun, nonEmptyData, JobPriority.NORMAL, null);
//
//        job = new GearmanJobImpl(validFun, validFun, false);
//        validateJob(job, validFun, objectToBytes(validFun), JobPriority.NORMAL,
//                null);
//
//        job = new GearmanJobImpl(validFun, validFun, false, JobPriority.LOW);
//        validateJob(job, validFun, objectToBytes(validFun), JobPriority.LOW,
//                null);
//
//
//        try {
//            job = new GearmanJobImpl(nullFun, nonEmptyData, false, JobPriority.LOW);
//            Assert.fail("Attempt to create a GearmanJob with null function" +
//                    " did not throw exception");
//        } catch (IllegalArgumentException expected) {
//        }
//
//        try {
//            job = new GearmanJobImpl(emptyFun, nonEmptyData, false, JobPriority.LOW);
//            Assert.fail("Attempt to create a GearmanJob with empty function" +
//                    " did not throw exception");
//        } catch (IllegalArgumentException expected) {
//        }
//
//        job = new GearmanJobImpl(validFun, nullData, false, JobPriority.LOW);
    }

//    @Test
//    public void jobHanldeTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        Assert.assertTrue("Job handle mismatch for accepted job",
//                Arrays.equals(handle, job.getHandle()));
//    }

//    @Test
//    public void jobStateTest()throws IOException {
//        GearmanJobImpl job = createInProgessJob("test".getBytes());
//        Assert.assertEquals("Job state mismatch for accepted job",
//                Status.INPROGRESS, job.getStatus());
//    }

//    @Test
//    public void getResultsTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        byte[] results = "S0m3R3sults!".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        byte[] packetPayload = new byte[handle.length + results.length + 1];
//        System.arraycopy(handle, 0, packetPayload, 0, handle.length);
//        packetPayload[handle.length] = ByteUtils.NULL;
//        System.arraycopy(results, 0, packetPayload, handle.length + 1,
//                results.length);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_COMPLETE,
//                packetPayload);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for completed job",
//                Status.COMPLETED, job.getStatus());
//        Assert.assertTrue("data returned by getResults does not match " +
//                "expected results", Arrays.equals(results, job.getResults()));
//        // call it again to verify that initial call has not cleared results
//        Assert.assertTrue("data returned by second getResults does not" +
//                " match expected results",
//                Arrays.equals(results, job.getResults()));
//
//    }

//    @Test
//    public void extractResultsTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        byte[] results = "S0m3R3sults!".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        byte[] packetPayload = new byte[handle.length + results.length + 1];
//        System.arraycopy(handle, 0, packetPayload, 0, handle.length);
//        packetPayload[handle.length] = ByteUtils.NULL;
//        System.arraycopy(results, 0, packetPayload, handle.length + 1,
//                results.length);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_COMPLETE,
//                packetPayload);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for completed job",
//                Status.COMPLETED, job.getStatus());
//        Assert.assertTrue("data returned by extractResults does not" +
//                " match expected results",
//                Arrays.equals(results, job.extractResults()));
//        // call it again to verify that extract call has cleared results
//        Assert.assertTrue("proceeding extractResults call did not return" +
//                " empty array", job.extractResults().length == 0);
//        Assert.assertTrue("proceeding getResults call did not return" +
//                " empty array", job.getResults().length == 0);
//
//    }

//    @Test
//    public void getWarningTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        byte[] results = "S0m3Warn1ng!".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        byte[] packetPayload = new byte[handle.length + results.length + 1];
//        System.arraycopy(handle, 0, packetPayload, 0, handle.length);
//        packetPayload[handle.length] = ByteUtils.NULL;
//        System.arraycopy(results, 0, packetPayload, handle.length + 1,
//                results.length);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_WARNING,
//                packetPayload);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for in progress job",
//                Status.INPROGRESS, job.getStatus());
//        Assert.assertTrue("data returned by getWarnings does not match" +
//                " expected results", Arrays.equals(results, job.getWarnings()));
//        // call it again to verify that initial call has not cleared results
//        Assert.assertTrue("data returned by second getWarnings does not " +
//                "match expected results",
//                Arrays.equals(results, job.getWarnings()));
//    }

//    @Test
//    public void extractWarningTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        byte[] results = "S0m3Warn1ng!".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        byte[] packetPayload = new byte[handle.length + results.length + 1];
//        System.arraycopy(handle, 0, packetPayload, 0, handle.length);
//        packetPayload[handle.length] = ByteUtils.NULL;
//        System.arraycopy(results, 0, packetPayload, handle.length + 1,
//                results.length);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_WARNING,
//                packetPayload);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for in progress job",
//                Status.INPROGRESS, job.getStatus());
//        Assert.assertTrue("data returned by extractWarnings does not match" +
//                " expected results",
//                Arrays.equals(results, job.extractWarnings()));
//        // call it again to verify that extract call has cleared results
//        Assert.assertTrue("proceeding extractWarnings call did not return" +
//                " empty array", job.extractWarnings().length == 0);
//        Assert.assertTrue("proceeding getWarnings call did not return" +
//                " empty array", job.getWarnings().length == 0);
//    }

//    @Test
//    public void getExceptionsTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        byte[] results = "S0m33xc3pt10n!".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        byte[] packetPayload = new byte[handle.length + results.length + 1];
//        System.arraycopy(handle, 0, packetPayload, 0, handle.length);
//        packetPayload[handle.length] = ByteUtils.NULL;
//        System.arraycopy(results, 0, packetPayload, handle.length + 1,
//                results.length);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_EXCEPTION,
//                packetPayload);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for in progress job",
//                Status.INPROGRESS, job.getStatus());
//        Assert.assertTrue("data returned by getExceptions does not match " +
//                "expected results", Arrays.equals(results, job.getExceptions()));
//        // call it again to verify that initial call has not cleared results
//        Assert.assertTrue("data returned by second getExceptions does not" +
//                " match expected results",
//                Arrays.equals(results, job.getExceptions()));
//    }

//    @Test
//    public void extractExceptionsTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        byte[] results = "S0m33xc3pt10n!".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        byte[] packetPayload = new byte[handle.length + results.length + 1];
//        System.arraycopy(handle, 0, packetPayload, 0, handle.length);
//        packetPayload[handle.length] = ByteUtils.NULL;
//        System.arraycopy(results, 0, packetPayload, handle.length + 1,
//                results.length);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_EXCEPTION,
//                packetPayload);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for in progress job",
//                Status.INPROGRESS, job.getStatus());
//        Assert.assertTrue("data returned by extractExceptions does not" +
//                " match expected results",
//                Arrays.equals(results, job.extractExceptions()));
//        // call it again to verify that extract call has cleared results
//        Assert.assertTrue("proceeding extractExceptions call did not return" +
//                " empty array", job.extractExceptions().length == 0);
//        Assert.assertTrue("proceeding getExceptions call did not return" +
//                " empty array", job.getExceptions().length == 0);
//    }

//    @Test
//    public void jobFailedTest() throws IOException {
//        byte[] handle = "test".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_FAIL, handle);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for in failed job",
//                Status.FAILED, job.getStatus());
//        Assert.assertTrue("isCompleted reports that a failed job is in a" +
//                " noncompleted state", job.isCompleted());
//    }

//    @Test
//    public void workStatusTest() throws IOException {
//        int ptr = 0;
//        byte[] handle = "test".getBytes();
//        GearmanJobImpl job = createInProgessJob(handle);
//        String num = "10";
//        String denom = "100";
//        byte[] numBytes = num.getBytes();
//        byte[] denomBytes = denom.getBytes();
//        byte[] packetPayload = new byte[handle.length + numBytes.length +
//                denomBytes.length + 2];
//        System.arraycopy(handle, 0, packetPayload, ptr, handle.length);
//        ptr += handle.length;
//        packetPayload[ptr++] = ByteUtils.NULL;
//        System.arraycopy(numBytes, 0, packetPayload, ptr, numBytes.length);
//        ptr += numBytes.length;
//        packetPayload[ptr++] = ByteUtils.NULL;
//        System.arraycopy(denomBytes, 0, packetPayload, ptr, denomBytes.length);
//        Packet p = new Packet(PacketMagic.RES, PacketType.WORK_STATUS,
//                packetPayload);
//        job.handleResponse(p);
//        Assert.assertEquals("Job state mismatch for in progress job",
//                Status.INPROGRESS, job.getStatus());
//        Assert.assertEquals("retrieved numerator does not equal expected",
//                Long.parseLong(num), job.getNumerator());
//        Assert.assertEquals("retrieved denominator does not equal expected",
//                Long.parseLong(denom), job.getDenominator());
//    }

//    @Test
//    public void nullSessionTest() {
//        GearmanJobImpl job = new GearmanJobImpl("fun", new byte[0], false);
//        try {
//            job.setSession(null);
//            Assert.fail("GearmanJob.setSession() did not throw an exception" +
//                    " when session variable was null");
//        } catch (IllegalArgumentException iae) {
//        } catch (Throwable t) {
//            Assert.fail("GearmanJob.setSession() threw an unexpected " +
//                    "exception " + t + " when session variable was null");
//        }
//    }
//
//    @Test
//    public void uinitializedSessionTest() {
//        GearmanJobImpl job = new GearmanJobImpl("fun", new byte[0], false);
//        try {
//            job.setSession(new GearmanJobServerSession(
//                    new InetSocketAddress(Constants.GEARMAN_DEFAULT_TCP_PORT)));
//            Assert.fail("GearmanJob.setSession() did not throw an exception" +
//                    " when session instance was not initialized");
//        } catch (IllegalArgumentException iae) {
//        } catch (Throwable t) {
//            Assert.fail("GearmanJob.setSession() threw an unexpected" +
//                    " exception " + t + " when session instance was not " +
//                    "initialized");
//        }
//    }
//
//    @Test
//    public void getRequestTest() {
//        String funName = "fun";
//        boolean[] detachedStates = new boolean[]{false, true};
//        byte[] data = new byte[]{'C', 'A', 'F', 'E', 'B', 'A', 'B', 'E'};
//        String id = "test";
//        for (boolean detached : detachedStates) {
//            for (JobPriority pr : JobPriority.values()) {
//                GearmanJobImpl job = new GearmanJobImpl(funName, data, detached, pr, id);
//                Packet p = job.getRequest();
//                Assert.assertEquals(PacketMagic.REQ, p.getMagic());
//                PacketType expectedType = null;
//                switch (pr) {
//                    case HIGH:
//                        if (detached) {
//                            expectedType = PacketType.SUBMIT_JOB_HIGH_BG;
//                        } else {
//                            expectedType = PacketType.SUBMIT_JOB_HIGH;
//                        }
//                        break;
//                    case NORMAL:
//                        if (detached) {
//                            expectedType = PacketType.SUBMIT_JOB_BG;
//                        } else {
//                            expectedType = PacketType.SUBMIT_JOB;
//                        }
//                        break;
//                    case LOW:
//                        if (detached) {
//                            expectedType = PacketType.SUBMIT_JOB_LOW_BG;
//                        } else {
//                            expectedType = PacketType.SUBMIT_JOB_LOW;
//                        }
//                }
//                Assert.assertEquals(expectedType, p.getPacketType());
//                ArrayList<byte[]> al = p.getDataComponents(3);
//                Assert.assertTrue(
//                        "Request function name doesnt match job function name",
//                        Arrays.equals(funName.getBytes(), al.get(0)));
//                Assert.assertTrue("Request id does not match job id",
//                        Arrays.equals(id.getBytes(), al.get(1)));
//                Assert.assertTrue("Request data does not match job data",
//                        Arrays.equals(data, al.get(2)));
//            }
//        }
//    }
//
//    @Test
//    public void getUpdateReguestTest() throws IOException {
//        String id = "test";
//        GearmanJobImpl job = createInProgessJob(id.getBytes());
//        Packet p = job.getUpdateRequest();
//        Assert.assertEquals("job update request packet has wrong magic",
//                PacketMagic.REQ, p.getMagic());
//        Assert.assertEquals("job update request packet has wrong type",
//                PacketType.GET_STATUS, p.getType());
//        Assert.assertTrue("job update request packet job handle does not" +
//                " match this jobs handle",
//                Arrays.equals(id.getBytes(), p.getData()));
//    }
//
//    private void validateJob(GearmanJobImpl job, String fname, byte[] data,
//            JobPriority priority, String id) {
//
//        Assert.assertEquals("Newly created job has unexpected status",
//                Status.NEW, job.getStatus());
//
//        Assert.assertTrue("Newly created job has unexpected job handle, " +
//                "handle should be null", job.getHandle() == null);
//
//        Assert.assertEquals("Newly created job has unexpected function name",
//                fname, job.getFunctionName());
//
//        Assert.assertEquals("Newly created job has unexpected priority",
//                priority, job.getPriority());
//
//        Assert.assertEquals("Newly created job has unexpected denominator", 0,
//                job.getDenominator());
//
//        Assert.assertEquals("Newly created job has unexpected numerator", 0,
//                job.getNumerator());
//
//        Assert.assertTrue("Newly created job has unexpected results",
//                job.getResults().length == 0);
//
//        Assert.assertTrue("Newly created job has unexpected warnings",
//                job.getWarnings().length == 0);
//
//        Assert.assertTrue("Newly created job has unexpected warnings",
//                job.getExceptions().length == 0);
//
//        if (id == null) {
//            Assert.assertTrue("Newly created job has unexpected priority",
//                    job.getID() != null);
//        } else {
//            Assert.assertEquals("Newly created job has unexpected description",
//                    GearmanJobImpl.DESCRIPTION_PREFIX + ":" + id + ":" + fname,
//                    job.toString());
//            Assert.assertEquals("Newly created job has unexpected priority",
//                    ByteUtils.fromUTF8Bytes(job.getID()), id);
//        }
//
//    }

    private byte[] objectToBytes(Serializable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream ostream = new ObjectOutputStream(out);
        ostream.writeObject(obj);
        ostream.close();
        return out.toByteArray();
    }

//    private GearmanJobImpl createInProgessJob(byte[] handle) throws IOException {
//        GearmanClientImpl gc = new GearmanClientImpl();
//        gc.addJobServer("localhost");
//        GearmanJobImpl j = new GearmanJobImpl("function", new byte[0], false,
//                JobPriority.LOW, "id");
//        Packet p = new Packet(PacketMagic.RES, PacketType.JOB_CREATED, handle);
//        j.setClient(gc);
//        j.handleResponse(p);
//        return j;
//    }
}
