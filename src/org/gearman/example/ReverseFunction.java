/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.example;

import org.gearman.client.GearmanJobResult;
import org.gearman.client.GearmanJobResultImpl;
import org.gearman.util.ByteUtils;
import org.gearman.worker.AbstractGearmanFunction;

public class ReverseFunction extends AbstractGearmanFunction {

    public GearmanJobResult executeFunction() throws Exception {

        StringBuffer sb = null;
        if (this.data instanceof byte[]) {
            sb = new StringBuffer(ByteUtils.fromUTF8Bytes((byte[]) this.data));
        } else {
            sb = new StringBuffer(data.toString());
        }
        GearmanJobResult gjr = new GearmanJobResultImpl(this.jobHandle,
                true, sb.reverse().toString().getBytes(),
                new byte[0], new byte[0], 0, 0);
        return gjr;
    }
}
