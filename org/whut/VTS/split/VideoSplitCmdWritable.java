package org.whut.VTS.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;

import org.apache.hadoop.io.Writable;
import org.whut.VTS.utils.ShareParamUtils;

public class VideoSplitCmdWritable implements Writable{
	//视频分割程序路径
	private String VideoSplitProcessPath;
	//视频起始时间参数
	private String StartTimeCmd;
	//视频分割持续时间参数
	private String DurationCmd;
	//视频文件
	private String InputFileCmd;
	
	private String OutputFileCmd;
	
	//其他参数
	private String args="";
	
	public VideoSplitCmdWritable(String vedioProcessPath, String startTimeCmd,
			String durationCmd, String inputFileCmd, String outputFileCmd,
			String[] args) {
		super();
		VideoSplitProcessPath = vedioProcessPath;
		StartTimeCmd = startTimeCmd;
		DurationCmd = durationCmd;
		InputFileCmd = inputFileCmd;
		OutputFileCmd = outputFileCmd;
		
		for(int i=0;i<args.length;i++){
			if(this.args!="")
				this.args+=ShareParamUtils.SEPARATE;
			this.args+=args[i];
		}
		
	}
	public VideoSplitCmdWritable() {
		super();
	}
	public String[] getCmd(long startTime,long durationTime,String inputfile,String outputfile){
		List<String> cmds=new ArrayList<String>();
		cmds.add(VideoSplitProcessPath);
		if(StartTimeCmd!="")
			cmds.add(StartTimeCmd);
		cmds.add(String.valueOf(startTime));
		if(DurationCmd!="")
			cmds.add(DurationCmd);
		cmds.add(String.valueOf(durationTime));
		if(InputFileCmd!="")
			cmds.add(InputFileCmd);
		cmds.add(inputfile);
		
		for(String value:args.split(ShareParamUtils.SEPARATE))
			cmds.add(value);
		
		if(OutputFileCmd!="")
			cmds.add(OutputFileCmd);
		cmds.add(outputfile);
		
		String[] vs=new String[cmds.size()];
		for(int i=0;i<cmds.size();i++)
			vs[i]=cmds.get(i);
		return vs;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		VideoSplitProcessPath=arg0.readUTF();
		StartTimeCmd=arg0.readUTF();
		DurationCmd=arg0.readUTF();
		InputFileCmd=arg0.readUTF();
		OutputFileCmd=arg0.readUTF();
		args=arg0.readUTF();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(VideoSplitProcessPath);
		arg0.writeUTF(StartTimeCmd);
		arg0.writeUTF(DurationCmd);
		arg0.writeUTF(InputFileCmd);
		arg0.writeUTF(OutputFileCmd);
		arg0.writeUTF(args);
	}
}

