/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package events;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class LoadNewClusters extends VoltProcedure {

    static final SQLStmt insertCluster = new SQLStmt(
            "INSERT INTO clusters VALUES (?,?,?,?,?)");

    static final SQLStmt truncateClusters = new SQLStmt("TRUNCATE TABLE CLUSTERS;");


    public long run(VoltTable loci) {
        voltQueueSQL(truncateClusters);

        loci.resetRowPosition();
        while (loci.advanceRow()) {
            voltQueueSQL(insertCluster,
                         EXPECT_SCALAR_MATCH(1),
                         loci.get(0, loci.getColumnType(0)),
                         loci.get(1, loci.getColumnType(1)),
                         loci.get(2, loci.getColumnType(2)),
                         loci.get(3, loci.getColumnType(3)),
                         loci.get(4, loci.getColumnType(4)));
        }

        voltExecuteSQL();

        return 0;
    }

}
