# VideoSummarySystem
This README would normally document whatever steps are necessary to get your application up and running.

### What is this repository for? ###

* Quick summary ：

MapReduce-based Video Summarization System

    1.  VideoLibarary: .cpp + openCV

    1.  VideoMapreduce: .java


### How do I get set up? ###

* Configuration 
Hadoop-2.7.0 
OpenCV2
Jdk 1.8.0

### Contribution guidelines ###

* Code review

    1. OpenCV+C++: 

        Processing the video file, calculate and obtain the video shot based on a certain threshold, generate a summary video with all the key frames (shot) of the video.

    1. JNI:

        Enable the VideoMapreduce(java project) to call the video summary library built from from VideoSummaryLib (C++ project).

    1. Hadoop Mapreduce: 

        Operates video with big data using Mapreduce programming model on Hadoop platform.

        3.1 Map: 

        parallel processing the video summary on the video split on each block.

        <filename, file frameoffset> -> <filename, list of (frameoffset +summarized files)>

        3.2 Reduce:

        Combine the summarized video files of the splits with the same filename.

        <filename, list of (frameoffset +summarized files)> -> <filename, result summary file>

        3.3 VideoInputFormat:

        Override the FileInputFormat class in Hadoop in order to keep the completeness of the video processing unit in the videoSummaryLib .


* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact
