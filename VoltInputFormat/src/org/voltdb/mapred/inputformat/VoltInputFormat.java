/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class VoltInputFormat extends InputFormat<LongWritable, Text> {

	int splitCount = 3;

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        VoltRecordReader rr = new VoltRecordReader();
        rr.initialize(split, arg1);
        return rr;
    }

    @Override
    public List<InputSplit> getSplits(JobContext ctx) throws IOException, InterruptedException {
    	List<InputSplit> retval = new ArrayList<>();

    	VoltClientShim volt = new VoltClientShim(ctx);

    	long partitionKeys[] = volt.getPartitionKeys();

    	for (long partitionKey : partitionKeys) {
    		long count = volt.queryPartition("select count(*) from events;", partitionKey).asScalarLong();

    		if (count == 0) {
    			continue; // avoid divide by zero... no work for this partition
    		}

    		long offsets[] = new long[splitCount];
    		long limits[] = new long[splitCount];

    		for (int i = 0; i < splitCount; i++) {
    			offsets[i] = i * (count / splitCount);
    		}
    		for (int i = 0; i < (splitCount - 1); i++) {
    			limits[i] = offsets[i + 1] - offsets[i];
    		}
    		limits[splitCount - 1] = count - offsets[splitCount - 1];

    		for (int i = 0; i < splitCount; i++) {
    			if (limits[i] == 0) {
    				continue; // no work to do
    			}

    			VoltInputSplit split = new VoltInputSplit(ctx, partitionKey, offsets[i], limits[i]);
    			retval.add(split);
    		}
    	}
    	volt.close();


    	//long count = volt.query("select count(*) from events").asScalarLong();
    	//volt.close();

    	/*for (int i = 0; i < count; i += count / splitCount) {
    		VoltInputSplit split = new VoltInputSplit(ctx, i, count / splitCount);
    		retval.add(split);
    	}*/
        return retval;
    }

}
