/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.mapred.inputformat;

import java.io.IOException;
import java.text.DateFormat;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.voltdb.VoltTable;

public class VoltRecordReader extends RecordReader<LongWritable, Text> {

	long partitionKey;
    long offset;
    long limit;
    long pos;
    String value;
    VoltTable result;
    VoltClientShim volt;
    int chunkSize = 100;
    boolean finished = false;
    DateFormat df = DateFormat.getDateTimeInstance();
    final String query = "select * from events order by ts, key, src limit {limit} offset {offset};";

    @Override
    public void close() throws IOException {
    	volt.close();
    }

    @Override
    public float getProgress() throws IOException {
        return limit / (float) (pos - offset);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return new LongWritable(offset + pos);
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
    	return new Text(value);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
    	VoltInputSplit voltSplit = (VoltInputSplit) split;
    	limit = voltSplit.limit;
    	offset = voltSplit.offset;
    	partitionKey = voltSplit.partitionKey;

        volt = new VoltClientShim(voltSplit.ctx);
    }

    boolean loadNextChunk() {
    	if (finished) {
    		return false;
    	}

    	if (offset != 0) {
    		@SuppressWarnings("unused")
			int x = 0;
    	}

    	if (pos >= limit) {
    		finished = true;
    		volt.close();
    		return false;
    	}

    	long myLimit = Math.min(chunkSize, limit - pos);

    	String myQuery = query.replace("{limit}", String.valueOf(myLimit));
        myQuery = myQuery.replace("{offset}", String.valueOf(offset + pos));

    	result = volt.queryPartition(myQuery, partitionKey);
    	int rowCount = result.getRowCount();

    	long keyMin = result.fetchRow(0).getLong(0);
    	long keyMax = result.fetchRow(result.getRowCount() - 1).getLong(0);

    	System.out.printf("QUERY (o:%d,l:%d) pos=%d / returned rows=%d / min=%d,max=%d: %s\n",
    			offset, limit, pos, rowCount, keyMin, keyMax, myQuery);

    	if (rowCount < chunkSize) {
    		finished = true;
    		volt.close();
    	}
    	return true;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while ((result == null) || !result.advanceRow()) {
        	if (!loadNextChunk()) {
        		return false;
        	}
        }
        pos++;

        // giant hack to convert rows to csv
        String[] csvParts = new String[result.getColumnCount()];
        for (int i = 0; i < result.getColumnCount(); i++) {
        	 Object objValue = result.get(i, result.getColumnType(i));
        	 if (objValue instanceof String) {
        		 objValue = "\"" + objValue + "\"";
        	 }
        	 csvParts[i] = result.wasNull() ? "NULL" : objValue.toString();
        }

        value = StringUtils.join(csvParts, ",");

        return true;
    }


}
