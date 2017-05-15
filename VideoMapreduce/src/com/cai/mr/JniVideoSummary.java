package com.cai.mr;

/********************************************************************************/
/* videoSummary based on openCV c++  										    */
/* (videoSummaryLib.cpp in VideoSummaryLib project) 			                */
/* there are two methods here:                                                  */
/* videoSummary(localPath): return localPath of the summarizedVideo.	        */
/* getFrameSize(localPath): return the basic frame_size of the video       		*/
/********************************************************************************/

public class JniVideoSummary {
	static {
		// 调用文件名为JNI Library.dll的动态库
		System.load("libVideoSummaryLib.dylib");
	}
	// native方法声明
	public native String videoSummary(String localPath);

	public native long getFrameSize(String localPath);

}
