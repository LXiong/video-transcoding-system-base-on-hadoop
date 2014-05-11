package org.whut.VTS.merge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.whut.VTS.utils.ShareParamUtils;

public class VideoMergeCmdWritable implements Writable{

	
	//视频合并程序路径
	private String VideoMergeProcessPath;
	
	//视频文件输出参数
	private String OutputFileCmd;
	
	//视频文件输入参数
	private String InputFileCmd;
	
	//其他参数
	private String args="";
	
	
	
	public VideoMergeCmdWritable(String videoMergeProcessPath, String outputFileCmd,
			String inputFileCmd, String []args) {
		super();
		VideoMergeProcessPath = videoMergeProcessPath;
		OutputFileCmd = outputFileCmd;
		InputFileCmd = inputFileCmd;
		
		for(int i=0;i<args.length;i++){
			if(this.args!="")
				this.args+=ShareParamUtils.SEPARATE;
			this.args+=args[i];
		}
	}

	public VideoMergeCmdWritable() {
		super();
	}

	public static String[] getCmd(String VideoMergeProcessPath,String OutputFileCmd,String InputFileCmd,String args,String []inputfiles,String outputfile){
		List<String> cmds=new ArrayList<String>();
		
		cmds.add(VideoMergeProcessPath);
		if(InputFileCmd!="")
			cmds.add(InputFileCmd);
		for(int i=0;i<inputfiles.length;i++)
			cmds.add(inputfiles[i]);
		for(String arg:args.split(ShareParamUtils.SEPARATE))
			cmds.add(arg);
		if(OutputFileCmd!="")
			cmds.add(OutputFileCmd);
		cmds.add(outputfile);
		
		String[] vs=new String[cmds.size()];
		for(int i=0;i<cmds.size();i++)
			vs[i]=cmds.get(i);
		return vs;
	}
	
	public static String getCmdToString(String VideoMergeProcessPath,String OutputFileCmd,String InputFileCmd,String args,String []inputfiles,String outputfile){
		String[] cmds=getCmd(VideoMergeProcessPath,OutputFileCmd,InputFileCmd,args,inputfiles,outputfile);
		String cmdString="";
		for(String cmd:cmds){
			cmdString+=cmd;
			cmdString+=" ";
		}
		return cmdString.trim();
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		VideoMergeProcessPath=arg0.readUTF();
		OutputFileCmd=arg0.readUTF();
		InputFileCmd=arg0.readUTF();
		args=arg0.readUTF();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(VideoMergeProcessPath);
		arg0.writeUTF(OutputFileCmd);
		arg0.writeUTF(InputFileCmd);
		arg0.writeUTF(args);
	}

	public String getVideoMergeProcessPath() {
		return VideoMergeProcessPath;
	}

	public void setVideoMergeProcessPath(String videoMergeProcessPath) {
		VideoMergeProcessPath = videoMergeProcessPath;
	}

	public String getOutputFileCmd() {
		return OutputFileCmd;
	}

	public void setOutputFileCmd(String outputFileCmd) {
		OutputFileCmd = outputFileCmd;
	}

	public String getInputFileCmd() {
		return InputFileCmd;
	}

	public void setInputFileCmd(String inputFileCmd) {
		InputFileCmd = inputFileCmd;
	}

	public String getArgs() {
		return args;
	}

	public void setArgs(String args) {
		this.args = args;
	}
}
