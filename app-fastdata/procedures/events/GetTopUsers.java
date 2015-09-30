/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.net.UnknownHostException;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class GetTopUsers extends VoltProcedure {
    final SQLStmt getTopSecond = new SQLStmt(
            "SELECT src, SUM(count_values) AS counts " +
            "FROM events_by_second " +
            "WHERE TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - ?) <= second_ts " +
            "GROUP BY src " +
            "ORDER BY counts DESC, src LIMIT ?;"
    );

    public VoltTable run(int seconds, int n) throws UnknownHostException
    {
        voltQueueSQL(getTopSecond, seconds, n);
        final VoltTable result = voltExecuteSQL(true)[0];
        final VoltTable.ColumnInfo[] schema = result.getTableSchema();
        schema[0] = new VoltTable.ColumnInfo("SRC", VoltType.STRING);
        final VoltTable processed = new VoltTable(schema);
        while (result.advanceRow()) {
            processed.addRow(Utils.itoip((int) result.getLong(0)),
                    result.getLong(1));
        }

        return processed;
    }
}
