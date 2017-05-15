package com.cai.mr;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;

/*********************************************************************************/
/* Encoded file into BytesWritable format for MapReduce                          */
/* encodedFile(frameOffset, summarized File): return combination of them into    */
/*                                        new BytesWrtable class.    		     */
/* decodeFile(frameOffset, summarized File): return EncodedFile with frameOffset */
/*                                         and its content    		             */
/*********************************************************************************/
public class EncodedFile {
	public long frameOffset;
	public BytesWritable summaryFile;

	/*
	 * 封装帧
	 * 
	 * @return BytesWritable
	 */
	public static BytesWritable encodeFile(long frameOffset, String summaryFilename) throws Exception {
		String tmp = frameOffset + ".";
		File file = new File(summaryFilename + ".avi");
		FileInputStream fin = new FileInputStream(file);
		BufferedInputStream bis = new BufferedInputStream(fin);  
		byte fileContent[] = new byte[(int) file.length()];
		bis.read(fileContent);
		byte[] buf = new byte[tmp.length() + fileContent.length];
		for (int i = 0; i < tmp.length(); ++i) {
			buf[i] = (byte) tmp.charAt(i);
		}
		for (int i = 0; i < fileContent.length; ++i) {
			buf[i + tmp.length()] = fileContent[i];
		}
        bis.close();   
		return new BytesWritable(buf);
	}
	/*
	 * 解封帧
	 * 
	 * @ return EncodedFile
	 */

	public static EncodedFile decodeFile(BytesWritable bw) throws IOException {
		EncodedFile eFile = new EncodedFile();
		int ind = -1;
		byte[] buf = bw.getBytes();
		for (int i = 0; i < buf.length; ++i) {
			if (buf[i] == (byte) '.') {
				ind = i;
				break;
			}
		}
		String strFrameOffset = "";
		for (int i = 0; i < ind; ++i) {
			strFrameOffset += (char) buf[i];
		}
		eFile.frameOffset = Long.valueOf(strFrameOffset);
		byte[] fileContent = new byte[buf.length - ind - 1];
		for (int i = 0; i < fileContent.length; ++i) {
			fileContent[i] = buf[i + (ind + 1)];
		}
		eFile.summaryFile = new BytesWritable(fileContent);
		return eFile;
	}

}
