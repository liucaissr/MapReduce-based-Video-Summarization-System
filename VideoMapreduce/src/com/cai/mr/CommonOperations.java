package com.cai.mr;

import java.io.File;

/********************************************************************************/
/* CommonOperations Class: 														*/	
/* getFileNameFromPath(): return filename from the LocalPath					*/
/* getExtFromPath(): return extension of the file								*/
/********************************************************************************/
public class CommonOperations {
    public static String getFileNameFromPath(String path) {
    	int ind = path.lastIndexOf(File.separator);
    	if (ind < 0 )
    		return path;
    	return path.substring(ind + 1);
    }
    
    public static String getExtFromPath(String path) {
    	int ind = path.indexOf('.');
    	return path.substring(ind + 1);
    }
    
    public static void main(String args[]) {
        System.out.println(getFileNameFromPath("/root/Downloads"));	
    }
}
