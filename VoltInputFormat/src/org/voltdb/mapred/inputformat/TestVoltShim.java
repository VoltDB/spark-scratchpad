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

package org.voltdb.mapred.inputformat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.junit.Test;
import org.voltdb.VoltTable;

import junit.framework.TestCase;

public class TestVoltShim extends TestCase {

	@Test
	public void testVoltShim() {
		VoltClientShim volt = new VoltClientShim("localhost");
		VoltTable t = volt.query("select * from events order by src desc limit 15;");
		System.out.println(t.toFormattedString());
		volt.close();
	}

	public void testInputFormat() throws IOException, InterruptedException {
		Job job = Job.getInstance();

		VoltInputFormat vif = new VoltInputFormat();
		List<InputSplit> splits = vif.getSplits(job);

		for (InputSplit split : splits) {
			RecordReader<LongWritable, Text> rr = vif.createRecordReader(split, null);

			while (rr.nextKeyValue()) {
				Text text = rr.getCurrentValue();
				String strRow = text.toString();
				System.out.println("ROW: " + strRow);
			}

			rr.close();
		}
	}
}
