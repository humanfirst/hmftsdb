package org.hmf.tsdb.demo;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;


public class DpsProducer {
	ObjectMapper mapper = new ObjectMapper();
	static Logger logger = LoggerFactory.getLogger(DpsProducer.class);
	private List<DataPoint> metrics = new ArrayList<DataPoint>();	
	static String[] metricName=new String[] {"temperature","humidity","voltage","current","power"};
	Producer<String, String> producer = null;
	int areaAmount=0,deviceAmount=0,metricAmount=0;
	
	public static void main(String[] args) {
		DpsProducer p = new DpsProducer();
		p.setUp();
		try {
			p.produce();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public void setUp() {
		Properties prop = new Properties();
        InputStream in = DpsProducer.class.getClassLoader().getResourceAsStream("demo.properties");
        try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
        areaAmount = Integer.valueOf(prop.getProperty("area.amount"));
        deviceAmount = Integer.valueOf(prop.getProperty("device.amount"));
        metricAmount = Integer.valueOf(prop.getProperty("metric.amount"));
        
		producer = new KafkaProducer<String, String>(prop);
		
		/*
		 * 初始化测点
		 */
		for(int i = 1;i<=areaAmount;i++) {
			for(int j=1;j<=deviceAmount;j++) {
				for(int k=1;k<=metricAmount;k++) {
					DataPoint dp = new DataPoint();
					TreeMap<String,String> tags = new TreeMap<String,String>();
					tags.put("area", "area_"+i);
					tags.put("device", "device_"+j);
					
					if(k<=5) {
						dp.setMetric(metricName[k-1]);
					}else {
						dp.setMetric("metric_"+k);
					}
					dp.setTags(tags);
					metrics.add(dp);
				}
			}
		}
		
	}
	
	public void produce() throws Exception {
		//采样间隔
		int sampleInterval = 1000;
		long preTime = 0;
		long count = 0;
		while(true){
			long now = System.currentTimeMillis();
			if(now-preTime>=(sampleInterval-5)) {
				logger.info("start to send dps to kafka...");
				preTime = now;
				for(DataPoint dp : metrics){
					float value = genValue(dp,now);
					dp.setValue(value);
					dp.setTime(now);
					doPut(dp);
					count++;
				}
				logger.info("has send {} dps to kafka",count);
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
	
	/**
	 * 产生某个测点在某个时候的模拟值
	 * @param metric
	 * @param time
	 * @return
	 */
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
		
		if(m.equals("voltage") || m.equals("current")||m.equals("power")||m.equals("temperature")||m.equals("humidity")) {
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
