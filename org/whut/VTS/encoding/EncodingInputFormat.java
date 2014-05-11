package org.whut.VTS.encoding;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//InputFormat接口的设计与实现

//用于描述输入数据的格式
//数据切分---按照某个策略将输入数据切分成若干个split，确定MapTask个数以及对应的split

//为Mapper提供输入数据:给定某个人split，能将其解析成一个个key/value对

public class EncodingInputFormat extends FileInputFormat<Text, FileSplit>{

	@Override
	public RecordReader<Text, FileSplit> createRecordReader(InputSplit split,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new RecordReader<Text, FileSplit>() {

			
			Text key;
			FileSplit value;
			boolean flag;
			@Override
			public void close() throws IOException {
				// TODO Auto-generated method stub
				
			}

			@Override
			public Text getCurrentKey() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				flag=false;
				return key;
			}

			@Override
			public FileSplit getCurrentValue() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return value;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public void initialize(InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				value=(FileSplit) arg0;
				
				//key值就是待转码的视频文件名字
				key=new Text(arg1.getConfiguration().get("videoName"));
				flag=true;
			}

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				return flag;
			}
		};
	}

	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		// TODO Auto-generated method stub
		return false;
	}		
}
