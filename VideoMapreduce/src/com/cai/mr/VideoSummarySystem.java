package com.cai.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import com.cai.mr.JniVideoSummary;
import com.google.common.primitives.Bytes;

/********************************************************************************/
/* MapReduce                                    								*/
/* Mapper:do videoSummary on each split.		                        	    */
/* input <Key, Value> :															*/
/* 		Key = local path of the split                        					*/
/* 		Value = frame offset of the split           							*/
/* output <Key, Value>         													*/
/* 		Key = filename of the split            									*/
/* 		Value = frameOffset + summarized split file           					*/
/* Reducer: 																	*/
/* input <Key, list(Value)> :													*/
/* 		Key = filename            												*/
/* 		Value = frameOffset + summarized split file              				*/
/* output <Key, Value>         													*/
/* 		Key = filename            												*/
/* 		Value = combined summarized file	                    				*/														
/********************************************************************************/

public class VideoSummarySystem {
	public static class VideoMapper extends MapReduceBase implements Mapper<Text, LongWritable, Text, BytesWritable> {
		@Override
		public void map(Text key, LongWritable offset, OutputCollector<Text, BytesWritable> collector,
				Reporter reporter) throws IOException {
			long frameOffset = offset.get();
			String filename = key.toString();
			String summaryFilename = new JniVideoSummary().videoSummary(filename);//filename without extension
			BytesWritable encodedFile;
			try {
				encodedFile = EncodedFile.encodeFile(frameOffset, summaryFilename);
				collector.collect(new Text(filename), encodedFile);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class VideoReducer extends MapReduceBase
			implements Reducer<Text, BytesWritable, Text, BytesWritable> {
		@Override
		public void reduce(Text key, Iterator<BytesWritable> files, OutputCollector<Text, BytesWritable> collector,
				Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			// 提取本组的帧
			ArrayList<EncodedFile> fileArray = new ArrayList<EncodedFile>();
			while (files.hasNext()) {
				EncodedFile eFile = EncodedFile.decodeFile(files.next());
				fileArray.add(eFile);
			}
			// 按帧号排序
			EncodedFile[] eFiles = new EncodedFile[fileArray.size()];
			eFiles = fileArray.toArray(eFiles);
			Arrays.sort(eFiles, new Comparator<EncodedFile>() {
				@Override
				public int compare(EncodedFile a, EncodedFile b) {
					return (int) (a.frameOffset - b.frameOffset);
				}
			});
			List<Byte> resultFileContent = new ArrayList<Byte>();
			for (int i = 0; i < eFiles.length; ++i) {
				byte fileContent[] = eFiles[i].summaryFile.copyBytes();
				for (Byte bt : fileContent) {
					resultFileContent.add(bt);
				}
			}
			byte[] resultBytes = Bytes.toArray(resultFileContent);
			collector.collect(key, new BytesWritable(resultBytes));
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		Runtime.getRuntime().exec("sh deleteOutput_video.sh"); // 清理上次运行的输出，才能正确运行程序
		Thread.sleep(1000);

		long startTime = System.nanoTime();
		JobConf conf = new JobConf(VideoSummarySystem.class);
		conf.setJobName("hadoop_video_summary_job");
		// 注意此设置要在设置 "OutputKeyClass", "OutputValueClass"之前。
		conf.setInputFormat(VideoInputFormat.class);
		conf.setOutputFormat(VideoOutputFormat.class);
		// conf.setOutputFormat(PictureOutputFormat.class);

		// 设置输入输出类，必须设置，否则出现类型不匹配错误！ Type Mismatch
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(BytesWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(BytesWritable.class);

		conf.setMapperClass(VideoMapper.class);
		// conf.setCombinerClass(PictureReducer.class);
		conf.setReducerClass(VideoReducer.class);

		/*
		 * args[0] = input folder: /data/video/input args[1] = output folder:
		 * /data/video/output
		 */
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// run
		JobClient.runJob(conf);
		long endTime = System.nanoTime();
		// display Results:
		System.out.println("Program runningTime = " + (endTime - startTime) / 1e9 + " Seconds.");
		System.out.println("Please view results from " + args[1]);
	}
}