package com.lihu;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

public class LocalTridentFunc {
	
	public static class PrintBolt extends BaseFunction{

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Integer value = tuple.getInteger(0);
			System.out.println(value);
		}
		
		
	}
	
	
	public static void main(String[] args) {
		FixedBatchSpout spout  = new FixedBatchSpout(new Fields("sentence"), 1, new Values(1));
		spout.setCycle(true);
		
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream("spout_id", spout) //new Stream 是Spout   each是bolt的设置
		.each(new Fields("sentence"), new PrintBolt(), new Fields(""));//设置一个Spout又设置一个Bolt 第三个参数后面没有接受 所以不需要指定
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("tridentTopology",new Config(), tridentTopology.build());
	}
	
	
	
	

}
