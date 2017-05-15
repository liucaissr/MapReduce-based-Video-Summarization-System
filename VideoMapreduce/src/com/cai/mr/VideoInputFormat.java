package com.cai.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.mortbay.log.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;

/********************************************************************************/
/* self-defined FileInputFormat for MapReduce                                   */
/* Override getSplits() method to keep the completeness of frame in the video 	*/
/* getSplits(): the file is divided into splits with size of integral multiple  */
/*			    of the basic frame_size of the video file.		     			*/
/********************************************************************************/

public class VideoInputFormat extends FileInputFormat<Text, LongWritable> {
	
	private long Frame_Size;

	protected boolean isSplitable(JobContext job, Path filename) throws IOException {
		for (FileStatus file : listStatus((JobConf) job)) {// filestatues是文件对应的信息，具体看对应的类
			Path path = file.getPath();
			Frame_Size = new JniVideoSummary().getFrameSize(path.toString());
			break;
		}
		return true;// 要求分片
	}

	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>(); // 用以存放生成的split的
		for (FileStatus file : listStatus((JobConf) job)) {// filestatues是文件对应的信息，具体看对应的类
			Path path = file.getPath();
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			long length = file.getLen(); // 得到文本的长度
			BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length); // 取得文件所在块的位置
			if ((length != 0) && isSplitable(job, path)) { // 如果文件不为空，并且可以分片的话就进行下列操作,
				long blockSize = file.getBlockSize();
				long splitSize = ((int) (blockSize / Frame_Size)) * Frame_Size;
				long bytesRemaining = length; // 文本的长度
				while (((double) bytesRemaining) / splitSize < splitSize) {// 剩下的文本长度小于splitsize时
					int blkIndex = getBlockIndex(blkLocations, length - bytesRemaining);// 找到对应block块中对应的第0个字符开始，
					splits.add(
							new FileSplit(path, length - bytesRemaining, splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}
				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));
				}
			} else if (length != 0) {
				splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
			} else {
				// Create empty hosts array for zero length files
				splits.add(new FileSplit(path, 0, length, new String[0]));
			}
		}
		Log.info("Total # of splits: " + splits.size());
		return splits;
	}

	@Override
	public RecordReader<Text, LongWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		try {
			return new VideoRecordReader(job, FileSplit.class.cast(split));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
