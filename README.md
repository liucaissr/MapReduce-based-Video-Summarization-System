# VideoSummarySystem
This README would normally document whatever steps are necessary to get your application up and running.

### What is this repository for? ###

* Quick summary ：

MapReduce-based Video Summarization System

* Repository：

    1.  VideoLibarary:  .cpp + openCV project

    1.  VideoMapreduce:  .java project


### How do I get set up? ###

* Configuration 

Hadoop-2.7.0 
OpenCV2
Jdk 1.8.0

### Contribution guidelines ###

#### Code review

1. OpenCV+C++: 

    Processing the video file, calculate and obtain the video shot based on a certain threshold, generate a summary video with all the key frames (shot) of the video.

1. JNI:

    Enable the VideoMapreduce(java project) to call the video summary library built from from VideoSummaryLib (C++ project).

1. Hadoop Mapreduce: 

    Operates video with big data using Mapreduce programming model on Hadoop platform.

    1. Map: parallel processing the video summary on the video split on each block.

        <filename, file frameoffset> -> <filename, list of (frameoffset +summarized files)>

    1. Reduce: combine the summarized video files of the splits with the same filename.

        <filename, list of (frameoffset +summarized files)> -> <filename, result summary file>

    1. VideoInputFormat: Override the FileInputFormat class in Hadoop in order to keep the completeness of the video processing unit in the videoSummaryLib .

