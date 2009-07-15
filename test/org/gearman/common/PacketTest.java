/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.common;


import org.gearman.util.ByteUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import junit.framework.Assert;
import org.junit.Test;

public class PacketTest {

  private static final byte [] BYTE_ARRAY_PATTER = {'D','E','A','D','B','E','E','F'};
  private static final int DEFAULT_NON_DATA_COMPONENT_MAX = 128;
  private static final int DEFAULT_DATA_COMPONENT = 8192;
  
  @Test
  public void testGetDataComponents() {
    for (int i = 0; i < 4; i++) {
      int [] sizes = generateComponentSizes(i);
      byte [] data = generatePacketData(sizes);
      GearmanPacketImpl p = new GearmanPacketImpl(GearmanPacketMagic.RES,GearmanPacketType.WORK_DATA,data);
      ArrayList <byte []> components = p.getDataComponents(i);
      Assert.assertTrue("componentSize mis-match. Pack data should have " + i + "components, but has " + components.size(),components.size() == i);
      Iterator<byte []> iter = components.iterator();
      int x = 0;
      while (iter.hasNext()) {
        Assert.assertTrue("Test iteration " + i + " detected PacketData corruption in component " + x, componentDataIsValid(iter.next()));
        x++;
      }
    }
  }
  
  private int [] generateComponentSizes(int numberOfComponents) {
    int [] sizes = new int [numberOfComponents];
    int i = 0;
    while (i < numberOfComponents) {
      int maxSize = i == numberOfComponents -1 ? DEFAULT_DATA_COMPONENT : DEFAULT_NON_DATA_COMPONENT_MAX;
      sizes[i]= (int) (Math.random() * maxSize);
      i++;
    }
    return sizes;
  }
  
  
  private byte[] generatePacketData(int [] componentSize) {
    byte [] pd = new byte [0];
    int numComponents = componentSize.length;
    int totalSize = numComponents - 1;
    int wptr = 0;

    if(numComponents == 0)
      return pd;
    
    for (int i = 0; i < numComponents; i++)
      totalSize += componentSize[i];

    pd = new byte[totalSize];
    for (int i = 0; i < numComponents; i++) {
        System.arraycopy(generateByteArray(componentSize[i]), 0, pd, wptr, componentSize[i]);
        wptr += componentSize[i];
        if (i != numComponents - 1)
          pd[wptr++] = ByteUtils.NULL;
    }
    return pd;
      
  }
    
  private byte [] generateByteArray (int size) {
    byte [] ra = new byte [size];
    int wptr = 0;
    int bytesToCopy = size;
    while (bytesToCopy != 0) {
      int len = bytesToCopy > BYTE_ARRAY_PATTER.length ? BYTE_ARRAY_PATTER.length : bytesToCopy;
      System.arraycopy(BYTE_ARRAY_PATTER, 0, ra, wptr, len);
      bytesToCopy -= len;
      wptr += len;
    }
    return ra;
    
  }
  
  private  boolean componentDataIsValid (byte [] data) {
    return Arrays.equals(data, generateByteArray(data.length));
    
  }
 
}
