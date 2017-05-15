package com.cai.mr;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/********************************************************************************/
/* self-defined FileRecordReader for MapReduce                                  */
/* Specify the <Key, Value> pair for Mapper 									*/
/* Key = local path of the file of the split            						*/
/* Value = frame offset of the split           									*/
/********************************************************************************/
public class VideoRecordReader implements RecordReader<Text, LongWritable> {

	byte[] buf;
	FileInputStream fin;
	public String localPath;
	public static final String exportPath = "/export/hdfs";

	private long frameOffset;
	private String fileName;
    private long pos;

	// Trying parsing the mounted local file path from hdfs file path.
	public static String getLocalPath(String hdfsPath) {
		if (hdfsPath.length() <= 7) {
			return hdfsPath;
		}
		int ind = hdfsPath.indexOf('/', 8);
		return exportPath + hdfsPath.substring(ind);
	}

	public VideoRecordReader(Configuration conf, FileSplit split) throws FileNotFoundException, Exception {
		frameOffset = split.getStart(); //frameOffset
		this.pos = frameOffset;
		fileName = split.getPath().toString();
		localPath = getLocalPath(fileName);
		buf = new byte[(int) split.getLength()];

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public Text createKey() {
		// TODO Auto-generated method stub
		return new Text();
	}

	@Override
	public LongWritable createValue() {
		// TODO Auto-generated method stub
		return new LongWritable();
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return pos;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(Text key, LongWritable value) throws IOException {
		key.set(localPath);
		value.set(frameOffset);
		return false;
	}

}
