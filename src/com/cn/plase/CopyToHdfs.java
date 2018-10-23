package com.cn.plase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToHdfs {
	private String uri1 = null;
	private String src1 = null;
	private String dst1 = null;
	public CopyToHdfs() throws IOException{
	ReadFile readfile = new ReadFile();
	uri1 =readfile.readValue("uri");
	src1 = readfile.readValue("src");
	dst1 = readfile.readValue("dst");
	/*下面进行转码，否则报非法字符的错误*/
	uri1= uri1.replaceAll(" ", "");
	src1= src1.replaceAll(" ", "");
	dst1= dst1.replaceAll(" ", "");
	}
public static void main(String[] args) throws Exception {
//添加需要运行的程序
	CopyToHdfs copy= new CopyToHdfs();
	copy.copyFromHdfs();
}

//从本地文件系统上传到HDFS文件系统
public  void copyFromHdfs() throws URISyntaxException, IOException {
	//获取文件系统的配置文件
	Configuration conf = new Configuration() ;
	URI uri =new URI(uri1);
	FileSystem fs = FileSystem.get(uri, conf) ;
	//创建一个本地文件对象
	Path src = new Path(src1);
	//创建一个hdfs的目录文件对象
	Path dst = new Path(dst1);
	//检测目录是否存在，如果不存在则创建一个新的文件价
	if(!(fs.exists(dst))){
	fs.mkdirs(dst);
	}
	//调用方法实现文件的上传
	fs.copyFromLocalFile(src, dst);
	//释放资源
	fs.close();
    }
}
	