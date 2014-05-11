package org.whut.VTS.merge;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.whut.VTS.utils.VTSUtil;

@Deprecated
public class EncodingReduce extends Reducer<Text, Text, NullWritable,NullWritable>{

	@Override
	protected void reduce(Text arg0, Iterable<Text> arg1,Context arg2)throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSystem hdfs=FileSystem.get(arg2.getConfiguration());
		FileSystem local=FileSystem.getLocal(arg2.getConfiguration());
		
		//获取待转码文件目录名字
		String videoDir=arg2.getConfiguration().get("videoDirOnHDFS");
		
		//本地下载文件夹
		String LocalDownloadDir="/"+arg2.getConfiguration().get("localdownloadDir","tmp")+"/"+videoDir+"_reduce";
		
		String videoCodec=arg2.getConfiguration().get("videoCodec","flv");
		
		//目标文件名字
		String outputfile=LocalDownloadDir+"/"+VTSUtil.getFileNameNoEx(arg0.toString())+"."+videoCodec;
		
		for(Text value:arg1){
			String videoName=new Path(value.toString()).getName();
			FSDataInputStream in=hdfs.open(new Path(value.toString()));
			IOUtils.copyBytes(in,local.create(new Path(LocalDownloadDir+"/"+videoName)), 2048,true);
		}
		
		//获取合并参数
		String VideoMergeProcessPath=arg2.getConfiguration().get("VideoMergeProcessPath","mencoder");
		
		String OutputFileCmd=arg2.getConfiguration().get("OutputFileCmd","-o");
		
		String InputFileCmd=arg2.getConfiguration().get("InputFileCmd", "");
		
		String args=arg2.getConfiguration().get("args","-ovc,copy,-oac,pcm");
		
		List<String> arrayinputfiles=new ArrayList<String>();
		
		for(File inputfile:new File(LocalDownloadDir).listFiles()){
			if(!VTSUtil.getExtensionName(inputfile.getName()).equals(videoCodec))
				continue;
				arrayinputfiles.add(inputfile.toString());
		}
		
		String[] inputfiles=new String[arrayinputfiles.size()];
		for(int i=0;i<arrayinputfiles.size();i++){
			inputfiles[i]=arrayinputfiles.get(i);
		}	
		
		String cmd=VideoMergeCmdWritable.getCmdToString(VideoMergeProcessPath, OutputFileCmd, InputFileCmd, args, inputfiles, outputfile);
		
		Process pc=Runtime.getRuntime().exec(/*VideoMergeCmdWritable.getCmd(VideoMergeProcessPath, OutputFileCmd, InputFileCmd, args, inputfiles, outputfile)*/cmd);
		
		pc.waitFor();
		
		IOUtils.copyBytes(local.open(new Path(outputfile)),hdfs.create(new Path(VTSUtil.getFileNameNoEx(arg0.toString())+"."+videoCodec)),arg2.getConfiguration());
		local.delete(new Path(LocalDownloadDir), true);
	}
}
