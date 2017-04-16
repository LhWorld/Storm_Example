package drpc;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

public class LocalStormDrpc {
	private static final Logger logger = Logger.getLogger(LocalStormDrpc.class);
	public static class MyBolt extends BaseRichBolt{
		private Map stormConf; 
		private TopologyContext context;
		private OutputCollector collector;
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.stormConf = stormConf;
			this.context = context;
			this.collector = collector;
		}
		/**
		 * tuple中会传递过来连个参数
		 * 第一个表示是请求的ID，第二个表示是请求的参数
		 */
		public void execute(Tuple input) {
			String value = input.getString(1);
			value = "hello1 "+value;
			logger.info("=======execute======"+value);
			logger.info("=======input.getValue(0)======"+input.getValue(0));
			this.collector.emit(new Values(input.getValue(0),value));//发送两个出去
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id","value"));
		}
	}
	
	
	public static void main(String[] args) {
		LinearDRPCTopologyBuilder linearDRPCTopologyBuilder = new LinearDRPCTopologyBuilder("mydrpc");//hello是函数名称
		linearDRPCTopologyBuilder.addBolt(new MyBolt());//指定Bolt
		
		LocalCluster localCluster = new LocalCluster();
		LocalDRPC drpc = new LocalDRPC();
		localCluster.submitTopology("drpc",new Config(), linearDRPCTopologyBuilder.createLocalTopology(drpc));
		
		
		
		String result = drpc.execute("mydrpc", "storm");//前一个是函数名称 后一个是传递的参数
		System.err.println("客户端调用结果："+result);
		
		
		
	}
	
	
	

}
