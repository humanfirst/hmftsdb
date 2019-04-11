package org.hmf.tsdb.server.bridge;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.server.service.DataPointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class KafkaBridge4HistoryData implements ApplicationListener<ContextRefreshedEvent> {
	static Logger logger = LoggerFactory.getLogger(KafkaBridge4HistoryData.class);
	static ObjectMapper mapper = new ObjectMapper();
	
	@Value("${kafka.consumer.bootstrap-servers}")
	private String servers;
	
	@Value("${kafka.consumer.group-id}")
	private String groupId;
	
	@Value("${kafka.consumer.topic}")
	private String topic;
	
	@Value("${kafka.consumer.thread.size}")
	private int threadSize;
	
	private boolean running = false;
	
	@Autowired
	DataPointService service;
	
	private Runnable waited = null;
	
	final class WaitPolicy implements RejectedExecutionHandler{
		@Override
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			waited = r;
		}
	}
	
	public void start() {
		logger.info("启动历史数据消费程序");
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", servers);
		props.setProperty("group.id", groupId);
		
		props.setProperty("enable.auto.commit", "false");
		props.setProperty("auto.offset.reset", "latest");
		
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(props);
		ThreadPoolExecutor executor = new ThreadPoolExecutor(threadSize, threadSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(20),new WaitPolicy());
		
		TopicPartition tp = new TopicPartition(this.topic, 0);
		List<TopicPartition> tps = Arrays.asList(tp);
        consumer.assign(tps);        
        
        for(TopicPartition one:tps) {
        	long offset = consumer.position(one);
        	consumer.seek(one, offset);
        }
       
		Thread t = new Thread(()->{
			try {
			    while(running) {
			    	try {
			    		List<DataPoint> dps = new ArrayList<DataPoint>();
				        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				        for (ConsumerRecord<String, String> record : records) {
			            	String msg = record.value();
							DataPoint dp = mapper.readValue(msg, DataPoint.class);
							dps.add(dp);
							
			            }
			            if(dps.size()>0) {
							executor.execute(()->{service.acceptDataPointList(dps);});
							while(waited!=null) {
								Runnable reTryTask = waited ;
								waited = null;
								executor.execute(reTryTask);
							}
						}
				        consumer.commitAsync(new OffsetCommitCallback() {
							@Override
							public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
									Exception exception) {								
								if(exception!=null){
									logger.info("提交offset失败!",exception);
								}
							}
				        });
			    	}catch(Exception e) {
			    		logger.error("kafka消息消费失败！",e);
			    		try {
							Thread.sleep(2000);
						} catch (InterruptedException e1) {
							logger.error("thread was interrupted",e1);
						}
			    	}
			    }
			} finally {
			  consumer.close();
			  executor.shutdown();
			}
		});
		t.start();
	}
	
	@PreDestroy
	public void destroy() {
		this.running = false;
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		this.running = true;
		this.start();
		
	}
}
