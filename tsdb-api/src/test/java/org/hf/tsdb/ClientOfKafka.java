package org.hf.tsdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hmf.tsdb.DataPoint;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;


public class ClientOfKafka {
	ObjectMapper mapper = new ObjectMapper();
	static Logger logger = LoggerFactory.getLogger(ClientTest.class);
	private List<DataPoint> metrics = new ArrayList<DataPoint>();	
	static String[] metricType=new String[] {"temperature","humidity","voltage","current","power"};
	Producer<String, String> producer = null;
	@Before
	public void setUp() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "127.0.0.1:9092");
		props.put("acks", "all");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<String, String>(props);
		

		for(int i=0;i<250;i++){
			for(int j=0;j<40;j++){
				DataPoint m = new DataPoint();
				TreeMap<String,String> tags = new TreeMap<String,String>();
				tags.put("device", "device_"+i);
				tags.put("mid", "metric_"+i+"_"+j);
				if(j<5)
					m.setMetric(metricType[j%5]);
				else
					m.setMetric("metric_"+(j%40));
				m.setTags(tags);
				metrics.add(m);
			}
		}
	}
	
	@Test
	public void testPutListOfDataPoint() throws Exception {
		//采样间隔
		int sampleInterval = 1000;
		long preTime = 0;
		while(true){
			long now = System.currentTimeMillis();
			if(now-preTime>=sampleInterval) {
				preTime = now;
				for(DataPoint dp : metrics){
					float value = genValue(dp,now);
					dp.setValue(value);
					dp.setTime(now);
					doPut(dp);
				}
				logger.info("发送一轮");
			}else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void doPut(DataPoint dp) throws Exception {
		String dpStr = mapper.writeValueAsString(dp);
		ProducerRecord<String, String> msg= new ProducerRecord<String, String>("tsdb-dps", 
				null,dpStr);
		producer.send(msg);
	}
	
	
	static Map<DataPoint,Object[]> parameters = new HashMap<DataPoint,Object[]>();
	private static float genValue(DataPoint metric ,long time){
		String m = metric.getMetric();
		Object[] ps = parameters.get(metric);
		if(ps==null) {
			ps = new Object[3];
			ps[0] = (int) (Math.round(Math.random()*5));
			
			if(m.equals("temperature")) {
				ps[1] = 4+Math.random()*4;
				ps[2] = (int) (18+Math.round(Math.random()*8));
			}else if(m.equals("humidity")) {
				ps[1] = 6+Math.random()*6;
				ps[2] = (int) (46+Math.round(Math.random()*8));
			}else if(m.equals("voltage")) {
				ps[1] = 10+Math.random()*10;
				ps[2] = (int) (210+Math.round(Math.random()*10));
			}else if(m.equals("current")) {
				ps[1] = 1 + Math.random()*1;
				ps[2] = (int) (5+Math.round(Math.random()*5));
			}else if(m.equals("power")) {
				ps[1] = 2 + Math.random()*2;
				ps[2] = (int) (10+Math.round(Math.random()*4));
			}else {
				ps[1] = 8 + Math.random() * 8;
				ps[2] = (int) (30+Math.round(Math.random()*20));
			}
			parameters.put(metric, ps);
		}
		
		double value = 0d;
		double d = 1.0 * (time+(Integer)ps[0]*60000) % (10*60*1000) / (10*60*1000);
		
		if(m.equals("voltage") || m.equals("current")||m.equals("power")) {
			double arc = 2* Math.PI * d % (2*Math.PI);
			value = (Integer)ps[2] + ((Double)ps[1]) * Math.sin(arc)/2;
			
		}else {			
			if(d<=0.5) {
				value = (Integer)ps[2] + (Double)ps[1]*d;
			}else {
				value = (Integer)ps[2] + (1.0* (Double)ps[1])/2 +  (Double)ps[1]*(0.5-d);
			}
		}
		
		return (float)value;
	}
	
}
