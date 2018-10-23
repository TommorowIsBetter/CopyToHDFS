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
	/*�������ת�룬���򱨷Ƿ��ַ��Ĵ���*/
	uri1= uri1.replaceAll(" ", "");
	src1= src1.replaceAll(" ", "");
	dst1= dst1.replaceAll(" ", "");
	}
public static void main(String[] args) throws Exception {
//�����Ҫ���еĳ���
	CopyToHdfs copy= new CopyToHdfs();
	copy.copyFromHdfs();
}

//�ӱ����ļ�ϵͳ�ϴ���HDFS�ļ�ϵͳ
public  void copyFromHdfs() throws URISyntaxException, IOException {
	//��ȡ�ļ�ϵͳ�������ļ�
	Configuration conf = new Configuration() ;
	URI uri =new URI(uri1);
	FileSystem fs = FileSystem.get(uri, conf) ;
	//����һ�������ļ�����
	Path src = new Path(src1);
	//����һ��hdfs��Ŀ¼�ļ�����
	Path dst = new Path(dst1);
	//���Ŀ¼�Ƿ���ڣ�����������򴴽�һ���µ��ļ���
	if(!(fs.exists(dst))){
	fs.mkdirs(dst);
	}
	//���÷���ʵ���ļ����ϴ�
	fs.copyFromLocalFile(src, dst);
	//�ͷ���Դ
	fs.close();
    }
}
	