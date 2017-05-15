package com.cai.mr;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.mortbay.log.Log;

/********************************************************************************/
/* self-defined FileRecordWriter for MapReduce:                                 */
/* Output the summarized video to the output folder                             */
/********************************************************************************/

public class VideoRecordWriter implements RecordWriter<Text, BytesWritable> {
	FSDataOutputStream fout;
	ByteArrayOutputStream baos;
	Path outPath;
	FileSystem fs;
	
	public VideoRecordWriter(FileSystem fs, JobConf job, String name, Progressable progress) throws IOException {
		outPath = FileOutputFormat.getTaskOutputPath(job, name);
		String strPath = outPath.toString();
		int ind = strPath.indexOf("/_temporary");
		strPath = strPath.substring(0, ind);
		outPath = new Path(strPath);
		this.fs = outPath.getFileSystem(job);
	}

	@Override
	public void close(Reporter arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(Text key, BytesWritable value) throws IOException {
		Path dfsPath = new Path(outPath.toString() + File.separator + CommonOperations.getFileNameFromPath(key.toString())+".avi");
		Log.info("outPath = " + outPath);
		Log.info("foutPath = " + dfsPath);
		fout = fs.create(dfsPath);
		fout.write(value.getBytes());
	    fout.close();
	}
}
