/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.worker;

import org.gearman.common.GearmanSessionEvent;
import org.gearman.common.GearmanPacketMagic;
import org.gearman.common.GearmanPacketType;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gearman.common.Constants;
import org.gearman.common.GearmanException;
import org.gearman.common.GearmanPacket;
import org.gearman.common.GearmanJobServerConnection;
import org.gearman.common.GearmanJobServerSession;
import org.gearman.common.GearmanTask;
import org.gearman.common.GearmanServerResponseHandler;
import org.gearman.common.GearmanSessionEventHandler;
import org.gearman.common.GearmanPacketImpl;
import org.gearman.util.ByteUtils;
import org.gearman.util.IORuntimeException;

public class GearmanWorkerImpl
        implements GearmanWorker, GearmanSessionEventHandler {

    static public enum State {

        IDLE, RUNNING, SHUTTINGDOWN
    }
    private static final String DESCRIPION_PREFIX = "GearmanWorker";
    private final String DESCRIPTION;
    private LinkedList<GearmanFunction> functionList = null;
    private Selector ioAvailable = null;
    private static final Logger LOG = Logger.getLogger(
            Constants.GEARMAN_WORKER_LOGGER_NAME);
    private String id;
    private HashMap<String, FunctionDefinition> functionMap;
    private State state;
    private ExecutorService executorService;
    private HashMap<GearmanJobServerSession, GearmanTask> taskMap = null;
    private HashMap<SelectionKey,GearmanJobServerSession> sessionMap = null;

    class GrabJobEventHandler implements GearmanServerResponseHandler {

        private GearmanJobServerSession session;
        private boolean isDone = false;

        GrabJobEventHandler(GearmanJobServerSession session) {
            super();
            this.session = session;
        }

        public void handleEvent(GearmanPacket event) throws GearmanException {
            handleSessionEvent(new GearmanSessionEvent(event, session));
            isDone = true;
        }

        public boolean isDone() {
            return isDone;
        }
    }

    class GearmanFunctionNameFactory implements GearmanFunctionFactory {

        private String className = null;

        GearmanFunctionNameFactory(String className) {
            this.className = className;
        }

        public String getFunctionName() {
            return className;
        }

        public GearmanFunction getFunction() {
            GearmanFunction f = null;
            try {
                Class c = Class.forName(className);
                Object o = c.newInstance();
                if (o instanceof GearmanFunction) {
                    f = (GearmanFunction) o;
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "Unable to create instance of " +
                        "Function: " + className, e);
            }
            return f;
        }
    }

    class FunctionDefinition {

        private long timeout = 0;
        private GearmanFunctionFactory factory = null;

        FunctionDefinition(long timeout, GearmanFunctionFactory factory) {
            this.timeout = timeout;
            this.factory = factory;
        }

        long getTimeout() {
            return timeout;
        }

        GearmanFunctionFactory getFactory() {
            return factory;
        }
    }

    public GearmanWorkerImpl() {
        this (null);
    }

    //For the time being this is private constructor because at this point in
    //time we are not supporting executors. When we support differnt
    //variants of the Execute services, we will open this up
    private GearmanWorkerImpl(ExecutorService executorService) {
        DESCRIPTION = DESCRIPION_PREFIX + ":" + Thread.currentThread().getId();
        functionList = new LinkedList<GearmanFunction>();
        id = DESCRIPTION;
        functionMap = new HashMap<String, FunctionDefinition>();
        state = State.IDLE;
        this.executorService = executorService;
        taskMap = new HashMap<GearmanJobServerSession, GearmanTask>();
        sessionMap = new HashMap<SelectionKey, GearmanJobServerSession>();
    }

    @Override
    public String toString() {
        return id;
    }

    public void work() {
        if (!state.equals(State.IDLE)) {
            throw new IllegalStateException("Can not call work while worker " +
                    "is running or shutting down");
        }

        state = State.RUNNING;
        while (isRunning()) {

            for (GearmanJobServerSession sess : sessionMap.values()) {
                int interestOps = SelectionKey.OP_READ;
                if (sess.sessionHasDataToWrite()) {
                    interestOps |= SelectionKey.OP_WRITE;
                }
                sess.getSelectionKey().interestOps(interestOps);
            }
            try {
                ioAvailable.select(1);
            } catch (IOException io) {
                LOG.log(Level.WARNING, "Receieved IOException while" +
                        " selecting for IO",io);
            }

             for (SelectionKey key : ioAvailable.selectedKeys()) {
                 GearmanJobServerSession sess = sessionMap.get(key);
                 if (sess == null) {
                     LOG.log(Level.WARNING,"Worker does not have " +
                             "session for key " + key);
                     continue;
                 }
                if (!sess.isInitialized()) {
                    continue;
                }
                try {
                    GearmanTask sessTask = taskMap.get(sess);
                    if (sessTask == null) {
                        sessTask = new GearmanTask(
                                new GrabJobEventHandler(sess),
                                new GearmanPacketImpl(GearmanPacketMagic.REQ,
                                GearmanPacketType.GRAB_JOB, new byte[0]));
                        taskMap.put(sess, sessTask);
                        LOG.log(Level.FINER,"Worker: " + this + " submitted a " +
                                sessTask.getRequestPacket().getPacketType() +
                                " to session: " + sess);
                    }
                    if (sessTask.getState().equals(GearmanTask.State.NEW)) {
                        sess.submitTask(sessTask);
                    }
                    sess.driveSessionIO();
                    //For the time being we will execute the jobs synchronously
                    //in the future, I expect to change this.
                    if (!functionList.isEmpty()) {
                        GearmanFunction fun = functionList.remove();
                        submitFunction(fun);
                    }
                } catch (IOException ioe) {
                    LOG.log(Level.WARNING, "Received IOException while driving" +
                            " IO on session " + sess, ioe);
                    sess.closeSession();
                    continue;
                }
            }
        }

        shutDownWorker(true);
    }

    public void handleSessionEvent(GearmanSessionEvent event)
            throws IllegalArgumentException, IllegalStateException {
        GearmanPacket p = event.getPacket();
        GearmanJobServerSession s = event.getSession();
        GearmanPacketType t = p.getPacketType();
        LOG.log(Level.FINER,"Worker " + this + " handling session event" +
                " ( Session = " + s + " Event = " + t + " )");
        switch (t) {
            case JOB_ASSIGN:
                taskMap.remove(s);
                addNewJob(event);
                break;
            case JOB_ASSIGN_UNIQ:
                taskMap.remove(s);
                addNewJob(event);
                break;
            case NOOP:
                taskMap.remove(s);
                break;
            case NO_JOB:
                taskMap.put(s, new GearmanTask(new GrabJobEventHandler(s),
                        new GearmanPacketImpl(GearmanPacketMagic.REQ,
                        GearmanPacketType.PRE_SLEEP, new byte[0])));
                break;
            case ECHO_RES:
                break;
            case OPTION_RES:
                break;
            case ERROR:
                s.closeSession();
                break;
        }
    }

    public void addServer(GearmanJobServerConnection conn)
            throws IllegalArgumentException, IllegalStateException {

        //this is a sub-optimal way to look for dups, but addJobServer
        //ops should be infrequent enough that this should be a big penalty
        for (GearmanJobServerSession sess : sessionMap.values()) {
            if (sess.getConnection().equals(conn)) {
                return;
            }
        }

        GearmanJobServerSession session =
                new GearmanJobServerSession(conn);
        if (ioAvailable == null) {
            try {
                ioAvailable = Selector.open();
            } catch (IOException ioe) {
                throw new IORuntimeException(ioe);
            }
        }
        try {
            session.initSession(ioAvailable, this);
        } catch (IOException ioe) {
            throw new IORuntimeException(ioe);
        }
        SelectionKey key = session.getSelectionKey();
        if (key == null) {
            String msg = "Session " + session + " has a null " +
                    "selection key. Server will not be added to worker.";
            LOG.log(Level.WARNING, msg);
            throw new IllegalStateException(msg);
        }
        sessionMap.put(key, session);

        GearmanPacket p = new GearmanPacketImpl(GearmanPacketMagic.REQ,
                GearmanPacketType.SET_CLIENT_ID, ByteUtils.toUTF8Bytes(id));
        session.submitTask(new GearmanTask(p));

        for (FunctionDefinition def : functionMap.values()) {
            p = generateCanDoPacket(def);
            session.submitTask(new GearmanTask(p));
        }

        p = new GearmanPacketImpl(GearmanPacketMagic.REQ,
                GearmanPacketType.GRAB_JOB, new byte[0]);
        GearmanTask gsr = new GearmanTask(
                new GrabJobEventHandler(session), p);
        taskMap.put(session, gsr);
        
        LOG.log(Level.FINE, "Added server " + conn + " to worker " + this);
    }

    public boolean hasServer(GearmanJobServerConnection conn) {
        boolean foundIt = false;
        for (GearmanJobServerSession sess : sessionMap.values()) {
            if (sess.getConnection().equals(conn)) {
                foundIt = true;
            }
        }
        return foundIt;
    }

    public String echo(String text, GearmanJobServerConnection conn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void registerFunction(String function, long timeout) {
        registerFunctionFactory(new GearmanFunctionNameFactory(function),
                timeout);
    }

    public void registerFunction(String function) {
        registerFunction(function, 0);
    }

    public void registerFunction(Class<? extends GearmanFunction> function) {
        registerFunction(function, 0);
    }

    public void registerFunction(Class<? extends GearmanFunction> function,
            long timeout) {
        registerFunctionFactory(new GearmanFunctionNameFactory(
                function.getName()), timeout);
    }

    public void registerFunctionFactory(GearmanFunctionFactory factory) {
        registerFunctionFactory(factory, 0);
    }

    public void registerFunctionFactory(GearmanFunctionFactory factory,
            long timeout) {
        if (functionMap.containsKey(factory.getFunctionName())) {
            return;
        }
        FunctionDefinition def = new FunctionDefinition(timeout, factory);
        functionMap.put(factory.getFunctionName(), def);
        sendToAll(generateCanDoPacket(def));
        LOG.log(Level.FINE, "Worker " + this + " has registered function " +
                factory.getFunctionName());
    }

    public Set<String> getRegisteredFunctions() {
        HashSet<String> functions = new HashSet<String>();
        for (FunctionDefinition def : functionMap.values()) {
            functions.add(def.factory.getFunctionName());
        }
        return functions;
    }

    public void setWorkerID(String id) throws IllegalArgumentException {
        if (id == null) {
            throw new IllegalArgumentException("Worker ID may not be null");
        }
        this.id = id;
        sendToAll(new GearmanPacketImpl(GearmanPacketMagic.REQ,
                GearmanPacketType.SET_CLIENT_ID, ByteUtils.toUTF8Bytes(id)));
    }

    public void setWorkerID(String id, GearmanJobServerConnection conn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getWorkerID() {
        return id;
    }

    public String getWorkerID(GearmanJobServerConnection conn) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void unregisterFunction(String functionName) {
        functionMap.remove(functionName);
        sendToAll(new GearmanPacketImpl(GearmanPacketMagic.REQ,
                GearmanPacketType.CANT_DO, ByteUtils.toUTF8Bytes(functionName)));
        LOG.log(Level.FINE, "Worker " + this + " has unregistered function " +
                functionName);
    }

    public void unregisterAll() {
        functionMap.clear();
        sendToAll(new GearmanPacketImpl(GearmanPacketMagic.REQ,
                GearmanPacketType.RESET_ABILITIES, new byte[0]));
    }

    public void stop() {
        state = State.SHUTTINGDOWN;
    }

    public List<Exception> shutdown() {
        return shutDownWorker(false);
    }

    public boolean isRunning() {
        return state.equals(State.RUNNING);
    }

    private GearmanPacket generateCanDoPacket(FunctionDefinition def) {
        GearmanPacketType pt = GearmanPacketType.CAN_DO;
        byte[] data = null;
        byte[] name = ByteUtils.toUTF8Bytes(def.getFactory().getFunctionName());
        long timeout = def.getTimeout();

        if (timeout > 0) {
            pt = GearmanPacketType.CAN_DO_TIMEOUT;
            byte[] to = ByteUtils.toUTF8Bytes(String.valueOf(timeout));
            data = new byte[name.length + to.length + 1];
            System.arraycopy(name, 0, data, 0, name.length);
            data[name.length] = ByteUtils.NULL;
            System.arraycopy(to, 0, data, name.length + 1, to.length);
        } else {
            data = name;
        }
        return new GearmanPacketImpl(GearmanPacketMagic.REQ, pt, data);
    }

    private void sendToAll(GearmanPacket p) {
        sendToAll(null, p);
    }

    private void sendToAll(GearmanServerResponseHandler handler, GearmanPacket p) {
        GearmanTask gsr = null;
        if (handler == null) {
            gsr = new GearmanTask(p);
        } else {
            gsr = new GearmanTask(handler, p);
        }
        for (GearmanJobServerSession sess : sessionMap.values()) {
            sess.submitTask(gsr);
        }

    }

    /*
     * For the time being this will always return an empty list of
     * exceptions because closeSession does not throw an exception
     */
    private List<Exception> shutDownWorker(boolean completeTasks) {
        LOG.log(Level.INFO,"Commencing shutdowm of worker " + this);

        ArrayList<Exception> exceptions = new ArrayList<Exception>();

        // This gives any jobs in flight a chance to complete
        if (executorService != null) {
            if (completeTasks) {
                executorService.shutdown();
            } else {
                executorService.shutdownNow();
            }
        }

        for (GearmanJobServerSession sess : sessionMap.values()) {
            sess.closeSession();
        }
        state = State.IDLE;
        LOG.log(Level.INFO, "Completed shutdowm of worker " + this);

        return exceptions;
    }

    private void addNewJob(GearmanSessionEvent event) {
        byte[] handle, data, functionNameBytes;
        GearmanPacket p = event.getPacket();
        GearmanJobServerSession sess = event.getSession();
        String functionName;
        handle = p.getDataComponentValue(
                GearmanPacket.DataComponentName.JOB_HANDLE);
        functionNameBytes = p.getDataComponentValue(
                GearmanPacket.DataComponentName.FUNCTION_NAME);
        data = p.getDataComponentValue(
                GearmanPacket.DataComponentName.DATA);
        functionName = ByteUtils.fromUTF8Bytes(functionNameBytes);
        FunctionDefinition def = functionMap.get(functionName);
        if (def == null) {
            GearmanTask gsr = new GearmanTask(
                    new GearmanPacketImpl(GearmanPacketMagic.REQ,
                    GearmanPacketType.WORK_FAIL, handle));
            sess.submitTask(gsr);
        }
        GearmanFunction function = def.getFactory().getFunction();
        function.setData(data);
        function.setJobHandle(handle);
        function.registerEventListener(sess);
        functionList.add(function);
    }

    private void submitFunction (GearmanFunction fun) {
        try {
            if (executorService == null) {
                fun.call();
            } else {
                Future<GearmanPacket> gp = executorService.submit(fun);
                FunctionDefinition def = functionMap.get(fun.getName());
                if (def == null) {
                    LOG.log(Level.WARNING, "Unable to find function " +
                            "execution attributes for function " +
                            fun.getName());
                }
                if (def != null && def.getTimeout() > 0) {
                    LOG.log(Level.FINER, "Worker:  " + this + " awaiting" +
                            " results of job " + gp + " with timout of " +
                            def.getTimeout() + " milliseconds");
                    gp.get(def.getTimeout(), TimeUnit.MILLISECONDS);
                } else {
                    LOG.log(Level.FINER, "Worker:  " + this + " awaiting" +
                            " results of job " + gp);
                    gp.get();
                }
            }
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Exception while getting function " +
                    "results", e);
            fun.fireEvent(new GearmanPacketImpl(GearmanPacketMagic.REQ,
                    GearmanPacketType.WORK_EXCEPTION,
                    GearmanPacketImpl.generatePacketData(
                    fun.getJobHandle(), e.getMessage().getBytes())));
            fun.fireEvent(new GearmanPacketImpl(GearmanPacketMagic.REQ,
                    GearmanPacketType.WORK_FAIL, fun.getJobHandle()));
        }
    }
}
