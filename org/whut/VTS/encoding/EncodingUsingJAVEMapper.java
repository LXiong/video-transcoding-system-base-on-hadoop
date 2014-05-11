package org.whut.VTS.encoding;

import it.sauronsoftware.jave.AudioAttributes;
import it.sauronsoftware.jave.Encoder;
import it.sauronsoftware.jave.EncoderException;
import it.sauronsoftware.jave.EncodingAttributes;
import it.sauronsoftware.jave.FFMPEGLocator;
import it.sauronsoftware.jave.InputFormatException;
import it.sauronsoftware.jave.VideoAttributes;
import it.sauronsoftware.jave.VideoSize;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.whut.VTS.utils.VTSUtil;

public class EncodingUsingJAVEMapper extends Mapper<Text, FileSplit, Text,Text>{
	@Override
	protected void map(Text key, FileSplit value,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		//统计分片计数
		context.getCounter("hadoop","splitNum").increment(1);
		
		//获取转码信息
		
		//使用的音频编码
		String audioCodec=context.getConfiguration().get("audioCodec", "libmp3lame");
		
		//音频比特率
		Integer audioBitRate=context.getConfiguration().getInt("audioBitRate", 64000);
		
		//音频声道数量
		Integer audioChannels=context.getConfiguration().getInt("audioChannels", 1);
		//音频采样频率
		Integer audioSamplingRate=context.getConfiguration().getInt("audioSamplingRate",22050);
		//视频编码方式
		String videoCodec=context.getConfiguration().get("videoCodec","flv");
		//视频比特率
		Integer videoBitRate=context.getConfiguration().getInt("videoBitRate", 160000);
		//视频帧率
		Integer videoFrameRate=context.getConfiguration().getInt("videoFrameRate", 15);
		//视频高
		Integer videoLength=context.getConfiguration().getInt("videoLength", 400);
		//视频宽
		Integer videoWidth=context.getConfiguration().getInt("videoWidth", 300);
		
		//获取待转码文件名字
		String videoName=value.getPath().getName();
		
		//获取待转码文件目录名字
		String videoDirOnHDFS=value.getPath().getParent().getName();
		
		
		//本地下载文件夹
		String LocalDownloadDir="/"+context.getConfiguration().get("localdownloadDir","tmp")+"/"+videoDirOnHDFS;
		
		//从HDFS中下载待转码文件到本地
		FileSystem hdfs=FileSystem.get(context.getConfiguration());
		FileSystem local=FileSystem.getLocal(context.getConfiguration());
		
		FSDataInputStream in=hdfs.open(value.getPath());
		
		local.createNewFile(new Path(LocalDownloadDir+"/"+videoName));
		IOUtils.copyBytes(in,local.create(new Path(LocalDownloadDir+"/"+videoName)), 2048,true);
		
		File source=new File(LocalDownloadDir+"/"+videoName);
		File target=new File(LocalDownloadDir+"/"+VTSUtil.getFileNameNoEx(videoName)+"."+videoCodec);
		
		AudioAttributes audio=new AudioAttributes();
		audio.setCodec(audioCodec);
		audio.setBitRate(new Integer(audioBitRate));
		audio.setChannels(new Integer(audioChannels));
		audio.setSamplingRate(new Integer(audioSamplingRate));
		
		VideoAttributes video=new VideoAttributes();
		video.setCodec(videoCodec);
		video.setBitRate(videoBitRate);
		video.setFrameRate(videoFrameRate);
		video.setSize(new VideoSize(videoWidth, videoLength));
		
		EncodingAttributes attrs=new EncodingAttributes();
		attrs.setFormat(videoCodec);
		attrs.setAudioAttributes(audio);
		attrs.setVideoAttributes(video);
		Encoder encoder=new Encoder(new FFMPEGLocator() {
			
			@Override
			protected String getFFMPEGExecutablePath() {
				// TODO Auto-generated method stub
				return "ffmpeg";
			}
		});
		
		try {
			encoder.encode(source, target, attrs);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InputFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (EncoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//上传转码后的视频
		IOUtils.copyBytes(local.open(new Path(LocalDownloadDir+"/"+VTSUtil.getFileNameNoEx(videoName)+"."+videoCodec)),hdfs.create(new Path(videoDirOnHDFS+"/"+VTSUtil.getFileNameNoEx(videoName)+"."+videoCodec)), 2048,true);
		
		context.write(key, new Text(videoDirOnHDFS+"/"+VTSUtil.getFileNameNoEx(videoName)+"."+videoCodec));
	}	
}
