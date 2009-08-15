/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.worker;


import org.gearman.client.GearmanIOEventListener;
import org.gearman.common.GearmanPacket;
import org.junit.Assert;
import org.junit.Test;


public class DefaultGearmanFunctionFactoryTest {

    class emptyNameFunction implements GearmanFunction {

        public String getName() {
            return " ";
        }

        public void setData(Object data) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void setJobHandle(byte[] handle) throws IllegalArgumentException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public byte[] getJobHandle() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void registerEventListener(GearmanIOEventListener listener)
                throws IllegalArgumentException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public void fireEvent(GearmanPacket event) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        public GearmanPacket call() throws Exception {
            throw new UnsupportedOperationException("Not supported yet.");
        }

    }

    @Test
    public void ctorTest() {
        GearmanFunction gf = new ExampleFunction();
        DefaultGearmanFunctionFactory factory =
                new DefaultGearmanFunctionFactory(
                gf.getClass().getCanonicalName());
        Assert.assertTrue(factory.getFunctionName().equals(gf.getName()));
        
        factory = new DefaultGearmanFunctionFactory("foobar", 
                gf.getClass().getCanonicalName());
        Assert.assertTrue(factory.getFunctionName().equals("foobar"));
    }

    @Test
    public void missingFunctionTest() {
        try {
            DefaultGearmanFunctionFactory factory =
                    new DefaultGearmanFunctionFactory("class.does.not.exist");
            Assert.fail();
        } catch (IllegalStateException expected) {}
    }

    @Test
    public void invalidFunctionTest() {
        try {
            DefaultGearmanFunctionFactory factory =
                    new DefaultGearmanFunctionFactory("java.lang.String");
            Assert.fail();
        } catch (IllegalStateException expected) {}
    }

    @Test
    public void getFunctionTest() {
        DefaultGearmanFunctionFactory factory =
                    new DefaultGearmanFunctionFactory(
                    ExampleFunction.class.getCanonicalName());
        GearmanFunction gf = factory.getFunction();
        Assert.assertNotNull(gf);
        Assert.assertTrue(gf instanceof ExampleFunction);

    }

    @Test
    public void nullFunctionNameTest() {
        DefaultGearmanFunctionFactory factory =
                    new DefaultGearmanFunctionFactory(
                    NullNameFunction.class.getCanonicalName());
        Assert.assertTrue(factory.getFunctionName().equals(
                NullNameFunction.class.getCanonicalName()));

    }

    @Test
    public void emptyFunctionNameTest() {
        DefaultGearmanFunctionFactory factory =
                    new DefaultGearmanFunctionFactory(
                    EmptyNameFunction.class.getCanonicalName());
        Assert.assertTrue(factory.getFunctionName().equals(
                EmptyNameFunction.class.getCanonicalName()));

    }
}
