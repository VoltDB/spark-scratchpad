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

package org.voltdb.spark;

import java.text.DateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.voltdb.mapred.inputformat.VoltInputFormat;
import org.voltdb.types.TimestampType;

import scala.Tuple2;

public class FastData {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("JavaWordCount");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration jobConf = new Configuration();
        jobConf.set("foo", "bar");

        JavaPairRDD<LongWritable, Text> rows = sc.newAPIHadoopRDD(jobConf,
                                                            VoltInputFormat.class,
                                                            LongWritable.class,
                                                            Text.class);
        final DateFormat dateFormat = DateFormat.getDateTimeInstance();
        JavaRDD<Row> rowRDD = rows.map(new Function<Tuple2<LongWritable, Text>, Row>() {
            private static final long serialVersionUID = -3803325282199497485L;
            @Override
            public Row call(Tuple2<LongWritable, Text> tuple) throws Exception {
            	String rowCSV = tuple._2().toString();
            	String[] parts = rowCSV.split(",");

            	Integer src = parts[0].equals("NULL") ? null : Integer.parseInt(parts[0]);
            	Integer dest = parts[1].equals("NULL") ? null : Integer.parseInt(parts[1]);
            	String method = parts[2].equals("NULL") ? null : parts[2];
            	TimestampType vts = parts[3].equals("NULL") ? null : new TimestampType(parts[3]);
            	Long millisFromEpoch = vts == null ? null : vts.getTime();
            	java.sql.Date ts = millisFromEpoch == null ? null : new java.sql.Date(millisFromEpoch);
            	Long key = parts[4].equals("NULL") ? null : Long.parseLong(parts[4]);
            	Long size = parts[5].equals("NULL") ? null : Long.parseLong(parts[5]);
            	Integer referral = parts[6].equals("NULL") ? null : Integer.parseInt(parts[6]);
            	Integer agent = parts[7].equals("NULL") ? null : Integer.parseInt(parts[7]);
            	Integer cluster =  parts[8].equals("NULL") ? null : Integer.parseInt(parts[8]);

                return RowFactory.create(src, dest, method, ts, key, size, referral, agent, cluster);
            }
        });

        MetadataBuilder metaBuilder = new MetadataBuilder();
        Metadata metadata = metaBuilder.build();

        StructField[] fields = new StructField[9];
        fields[0] = new StructField("src", DataTypes.IntegerType, false, metadata);
        fields[1] = new StructField("dest", DataTypes.IntegerType, false, metadata);
        fields[2] = new StructField("method", DataTypes.StringType, false, metadata);
        fields[3] = new StructField("ts", DataTypes.DateType, false, metadata);
        fields[4] = new StructField("key", DataTypes.LongType, false, metadata);
        fields[5] = new StructField("size", DataTypes.LongType, false, metadata);
        fields[6] = new StructField("referral", DataTypes.IntegerType, false, metadata);
        fields[7] = new StructField("agent", DataTypes.IntegerType, false, metadata);
        fields[8] = new StructField("cluster", DataTypes.IntegerType, false, metadata);
        StructType schema = new StructType(fields);

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.createDataFrame(rowRDD, schema);

        df.show();

        df.printSchema();

        for (String s : df.columns()) {
            System.out.println(s);
        }

        Column col = df.col("key");
        Column cond = col.gt(7);
        DataFrame df2 = df.filter(cond);
        df2.show();

        //df.filter(df.col("key").gt(7)).show();

        df.registerTempTable("events");
        sqlContext.cacheTable("events");

        DataFrame results;
        String sql;

        sql = "select * from events where key > 98 order by key asc";
        System.out.println(sql);
        results = sqlContext.sql(sql);
        results.show();

        sql = "select * from events where key = 7";
        System.out.println(sql);
        results = sqlContext.sql(sql);
        results.show();

        sql = "select count(*) as counttotal from events";
        System.out.println(sql);
        results = sqlContext.sql(sql);
        results.show();

        sql = "select count(distinct src) as countdistinct from events";
        System.out.println(sql);
        results = sqlContext.sql(sql);
        results.show();

        sql = "select min(src), max(src) from events";
        System.out.println(sql);
        results = sqlContext.sql(sql);
        results.show();

        sc.stop();
        sc.close();
    }

}
