package org.hf.tsdb;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.hmf.tsdb.Client;
import org.hmf.tsdb.ClientFactory;
import org.hmf.tsdb.DataPoint;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTest {
	static Logger logger = LoggerFactory.getLogger(ClientTest.class);
	private ClientFactory cf;
	private List<DataPoint> metrics = new ArrayList<DataPoint>();	
	static String[] metricType=new String[] {"temperature","humidity","voltage","current","power"};
	
	@Before
	public void setup() {
		cf = new ClientFactory();
		
		cf.setHost("127.0.0.1");
		cf.setPort(7677);
		cf.setMaxConnections(20);

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
	
	private void doPut(List<DataPoint> dps) {
		Client client = null;
		try {
			client = cf.getClient();
			System.out.println(client);
			client.put(dps);
			client.flush();
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			if(client!=null)
				client.release();
		}
	}
	
	@Test
	public void testQuery() {
		Client client = null;
		try {
			 client = cf.getClient();
			 //String result = client.query("current{device='device_1'} last[1h]");
			 String result =client.query("current{device='device_1'} from '2019-03-01 01:50:00' to '2019-03-01 02:50:00'");
			 System.out.println(result);
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			if(client!=null)
				client.release();
		}
	}
	
	@Test
	public void testPutListOfDataPoint0() {
		//采样间隔
		int sampleInterval = 1000;
		long preTime = 0;
		while(true){
			long now = System.currentTimeMillis();
			if(now-preTime>=sampleInterval) {
				preTime = now;
				List<DataPoint> dps = new ArrayList<DataPoint>();
				int count = 0;
				for(DataPoint dp : metrics){
					float value = genValue(dp,now);
					dp.setValue(value);
					dp.setTime(now);
					dps.add(dp);
					count++;
					if(count%1000==0) {
						doPut(dps);
						dps.clear();
					}
				}
				if(dps.size()>0) {
					doPut(dps);
					dps.clear();
				}
				
			}else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void testPutListOfDataPoint() {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long startLong = 0;
		long endLong = 0;
		try {
			//startLong = df.parse("2019-03-01 01:50:00").getTime();
			//endLong = df.parse("2019-03-30 00:00:00").getTime();
			startLong = System.currentTimeMillis();
			endLong = Long.MAX_VALUE;
		}catch(Exception e) {
			e.printStackTrace();
		}
		//采样间隔
		int sampleInterval = 1; 
		//加速倍率
		int rate = 1;  
		long sampleAt = startLong;
		long preTime = 0;
		int period = sampleInterval*1000/rate;
		while(sampleAt<endLong){
			long now = System.currentTimeMillis();
			if(now-preTime>=(period-10)) {
				preTime = now;
				List<DataPoint> dps = new ArrayList<DataPoint>();
				int count = 0;
				for(DataPoint dp : metrics){
					float value = genValue(dp,sampleAt);
					dp.setValue(value);
					dp.setTime(sampleAt);
					dps.add(dp);
					count++;
					if(count%1000==0) {
						doPut(dps);
						dps.clear();
					}
				}
				if(dps.size()>0) {
					doPut(dps);
					dps.clear();
				}
				System.out.println("采样时间："+df.format(sampleAt));
				sampleAt+=sampleInterval*1000;
			}else {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
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
