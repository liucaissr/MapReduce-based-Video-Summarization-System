package com.cai.mr;

import java.io.IOException;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.mortbay.log.Log;

/********************************************************************************/
/* self-defined FileOutputFormat for MapReduce:                                 */
/* Output the summarized video to the output folder                             */
/********************************************************************************/

public class VideoOutputFormat extends FileOutputFormat<Text, BytesWritable> {

	@Override
	public org.apache.hadoop.mapred.RecordWriter<Text, BytesWritable> getRecordWriter(FileSystem fs, JobConf job,
			String name, Progressable progress) throws IOException {
		// TODO Auto-generated method stub
		Log.info("outPath = " + FileOutputFormat.getTaskOutputPath(job, name).toString());
		return new VideoRecordWriter(fs, job, name, progress);
	}
}
