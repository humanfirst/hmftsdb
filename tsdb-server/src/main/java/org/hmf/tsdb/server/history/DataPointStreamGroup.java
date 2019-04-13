package org.hmf.tsdb.server.history;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.types.ObjectId;
import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.server.dao.DpsInHourDAO;
import org.hmf.tsdb.server.dao.SnapshotDAO;
import org.hmf.tsdb.server.entity.DpsInHour;
import org.hmf.tsdb.server.entity.Fragment;
import org.hmf.tsdb.server.entity.Metric;
import org.hmf.tsdb.server.entity.Snapshot;
import org.hmf.tsdb.server.exception.TimeBoundException;
import org.hmf.tsdb.server.service.MetricService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;


@Component
@Scope("prototype")
public class DataPointStreamGroup {
	static Logger logger = LoggerFactory.getLogger(DataPointStreamGroup.class);
	
	@Value("${tsdb.stream.size}")
	private int streamSize= 3600;
	
	@Value("${tsdb.ext.stream.size}")
	private int extStreamSize= 360;
	
	@Value("${tsdb.snapshot.interval}")
	private int snapshotInterval= 10;
	
	@Value("${tsdb.snapshot.clean.interval}")
	private int snapshotCleanInterval= 10;
	
	@Value("${tsdb.snapshot.clean.batch.size}")
	private int snapshotCleanBatchSize= 10;
	
	/**
	 * 分组序号
	 */
	private int idx = 0;
	
	private Map<Integer,DataPointStream> streams = new ConcurrentHashMap<Integer,DataPointStream>();
	
	/**
	 * 已经结束等待写入数据库的stream;
	 */
	private ConcurrentLinkedQueue<DataPointStream> finishedStreams = new ConcurrentLinkedQueue<DataPointStream>();
	
	@Autowired
	MetricService metricService = null;
	
	@Autowired
	DpsInHourDAO dpsInHourDAO = null;
	
	@Autowired
	SnapshotDAO snapshotDAO = null;
	
	private ConcurrentHashMap<Integer,Thread> writeFlags = new ConcurrentHashMap<Integer,Thread>();
	
	/**
	 * 快照锁，当有线程在执行快照的时候，不允许其他线程执行快照任务。注：快照线程和归档线程都可能执行快照任务
	 */
	private AtomicBoolean snapshotLock = new AtomicBoolean(false);
	
	
	private Timer timer4File = null;
	private Timer timer4Snapshot = null;
	private boolean stopped = false;
	
	
	/**
	 * 锁定某个测点的datapointStream ，不允许操作
	 * @param metricId
	 * @return
	 */
	private boolean lockStream(Integer metricId) {
		Thread lockedBy = null;
		for(;;) {
			lockedBy = writeFlags.putIfAbsent(metricId,Thread.currentThread());
			if(lockedBy==null) {
				return true;
			}else {
				logger.debug("the data array of metric[id={}] is locked by {}",metricId,lockedBy);
			}
		}
	}
	private void unlockStream(Integer metricId) {
		writeFlags.remove(metricId);
	}
	
	
	protected void copyExtStreamToMainStream(DataPointStream mainStream,DataPointStream extStream) {
		mainStream.reset();
		if(extStream!=null && extStream.getCurrentHour()>0) {
			System.arraycopy(extStream.getBytes(), 0, mainStream.getBytes(), 0, extStream.getPointer());
			mainStream.setMetric(extStream.getMetric());
			mainStream.setStatus(DataPointStream.Status.working);
			extStream.reset();
		}
	}
	
	private int idleTime  = 0;
	private boolean isFiling = false;
	private void doFileInHour() {
		DataPointStream stream = null;
		int count = 0;
		
		while((stream = finishedStreams.poll())!=null) {
			//标记为已开始归档
			if(!isFiling)
				isFiling = true;
			
			this.lockStream(stream.getMetric().getId());
			try {
				//保存主Stream
				DpsInHour one = new DpsInHour(stream);
				if(!dpsInHourDAO.exists(one.getId()))
					dpsInHourDAO.create(one);
				//把备用Stream复制到主stream
				copyExtStreamToMainStream(stream,this.streams.get(0-stream.getMetric().getId()));
			} catch (Exception e) {
				logger.error("对测点的时序数据 按小时归档失败！",e);
			}finally {
				this.unlockStream(stream.getMetric().getId());
			}
			count++;
		}
		
		if(count==0) {
			if(isFiling){
				idleTime++;
				if(idleTime>=5) {
					//轮空5次，则认为本次归档结束，做一次全量快照
					doFullSnapshot();
					isFiling=false;
				}
			}
		}else {
			logger.info("已按小时归档了{}个测点的时序数据",count);
		}
	}

	public void init() {
		loadSnapshots();
		timer4File = new Timer("归档定时器-组"+this.idx);
		timer4File.schedule(new TimerTask() {
			@Override
			public void run() {
				doFileInHour();
			}
		}, 3000, 400);
		
		timer4Snapshot = new Timer("快照定时器-组"+this.idx);
		timer4Snapshot.schedule(new TimerTask() {
			public void run() {
				doSnapshot();
			}
		}, 3000,this.snapshotInterval/3);
	}
	
	
	public void destory() {
		this.stopped = true;
		while(finishedStreams.size()>0) {
			logger.info("等待归档完成");
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				logger.error("休眠线程被中断！",e);
			}
		}
		logger.info("关闭归档定时器！");
		this.timer4File.cancel();
		
		
		logger.info("关闭快照定时器！");
		this.timer4Snapshot.cancel();
	}
	
	

	public DataPointStream getStream(Metric metric) {
		DataPointStream stream = streams.get(metric.getId());
		if(stream==null) {
			stream = new DataPointStream(metric,streamSize);
			streams.put(metric.getId(), stream);
		}
		return stream;
	}
	
	public DataPointStream getExtStream(Metric metric) {
		Integer key = 0-metric.getId();
		DataPointStream stream = streams.get(key);
		if(stream==null) {
			stream = new DataPointStream(metric,streamSize);
			streams.put(key, stream);
		}
		return stream;
	}
	

	public void acceptDataPoint(DataPoint dp) {
		//测点处理
		Metric m = metricService.findAndCreateMetric(dp);
		
		//值类型转换
		Number number = dp.getValue();
		if(number instanceof Double || number instanceof Float) {
			Float  f = number.floatValue();
			dp.setValue(f);
		}
		
		this.lockStream(m.getId());
		//找到stream，并写入dp
		DataPointStream stream = null;
		DataPointStream extStream = null;
		try {
			try {
				stream = this.getStream(m);
				if(stream.getStatus().equals(DataPointStream.Status.working)) {
					stream.write(dp);
				}else {
					extStream = this.getExtStream(m);
				}
			} catch (TimeBoundException e) {
				stream.setStatus(DataPointStream.Status.finished);
				this.finishedStreams.offer(stream);
				extStream = this.getExtStream(m);					
			} 
			//如果切换到extStream，则把dp写入extStream
			if(extStream!=null) {
				extStream.write(dp);
			}
		}catch(Exception e) {
			logger.error("写入DP失败！",e);
		}finally {
			this.unlockStream(m.getId());
		}
	}
	
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public void acceptDataPointList(List<DataPoint> dps) {		
		for(DataPoint dp:dps) {
			this.acceptDataPoint(dp);
		}
	}
	
	private void loadFullSnapshot(List<Snapshot> list) {
		Fragment fragment = null;
		for(Snapshot s:list) {
			while((fragment=s.nextFramgment())!=null) {
				Metric metric = this.metricService.findMetricById(fragment.metricId);
				DataPointStream stream = new DataPointStream(metric,this.streamSize);
				System.arraycopy(fragment.data, 0, stream.getBytes(), 0, fragment.data.length);
				this.streams.put(metric.getId(), stream);
				stream.setPointer(fragment.data.length);
				stream.setPreSnapshotAt(fragment.data.length);
				if(metric.getId().intValue()==46000) {
					logger.info("sample hour = {}",new Date(stream.readHour()*3600000));
				}
			}
		}
	}
	
	private void loadAdditionSnapshot(Snapshot ss) {
		Fragment fragment = null;
		while((fragment=ss.nextFramgment())!=null) {
			Metric metric =  this.metricService.findMetricById(fragment.metricId);
			DataPointStream stream = this.getStream(metric);
			stream.write(fragment);
		}
	}
	
	private void loadSnapshots() {
		List<Snapshot> list = snapshotDAO.findLastFullSnapshot(idx);		
		if(list!=null && list.size()>0) {
			//尝试删除可能存在的 已过期的快照数据
			ObjectId lastFullSnapshotId = list.get(0).getId();
			this.deleteTimeoutSnapshots(lastFullSnapshotId);
			logger.info("开始导入组{}的全量快照数据",this.idx);
			this.loadFullSnapshot(list);
			logger.info("组{}的全量快照数据加载完成",idx);
			
			logger.info("开始导入组{}的增量快照数据",idx);
			int batchSize = 10;
			List<Snapshot> additions = snapshotDAO.findAdditionSnapshots(this.idx,list.get(0).getId(),batchSize);
			list.clear(); list = null;
			
			if(additions.size()>0) {					
				for(;;) {
					for(Snapshot ss:additions) {
						this.loadAdditionSnapshot(ss);
					}
					
					if(additions.size()==batchSize) {							
						ObjectId after = additions.get(batchSize-1).getId();
						additions.clear(); additions = null;
						additions = snapshotDAO.nextPageSnapshots(this.idx,after, batchSize);
						try {
							Thread.sleep(10);
						} catch (InterruptedException e) {
							logger.error("",e);
						}
					}else
						break;
				}
			}
		}		
		logger.info("组{}的快照数据已经全部导入",this.idx);		
	}
	
	private boolean lockSnapshot(boolean canIgnore) {
		boolean success = false;
		if(!canIgnore) {
			for(;;) {
				success = snapshotLock.compareAndSet(false, true);
				if(success) {
					break;
				}else {
					try {
						Thread.sleep(10);
					}catch(Exception e) {
						logger.error("",e);
					}
				}
			}
		}else {
			success = snapshotLock.compareAndSet(false, true);
		}
		return success;
	}
	
	private void unloackSnapshot() {
		snapshotLock.set(false);
	}
	
	private long preSnapshotAt = 0;	
	private void doSnapshot() {
		long now = System.currentTimeMillis();
		if(now-preSnapshotAt>this.snapshotInterval*1000) {			
			//如果数据库中没有全量快照，则执行全量快照；否则执行增量快照
			if(this.snapshotDAO.existFullSnapshot(this.idx)) {
				this.doAdditionSnapshot();
			}else {
				this.doFullSnapshot();
			}
		}
	}
	
	/**
	 * 
	 * @param ss
	 * @param stream
	 * @return true:success ;false:snapshot is full.
	 * @throws Exception 
	 */
	private Snapshot writeAdditionSnapshot(Snapshot ss,DataPointStream stream) throws Exception{
		int preSnapPos = stream.getPreSnapshotAt();
		int tail = stream.getPointer();
		
		if(tail>preSnapPos) {
			int len = tail - preSnapPos;
			if(ss.restSize() < len) {		
				Snapshot saved = this.snapshotDAO.create(ss);
				logger.info("写入增量快照：{}",saved);
				Snapshot newPart = new Snapshot();
				newPart.setTime(saved.getTime());
				newPart.setIndex(saved.getIndex());
				newPart.setType(Snapshot.Type.part);
				newPart.setGroupIdx(this.idx);
				newPart.setParent(saved.getParent()==null?saved.getId():saved.getParent());
				ss = newPart;
			}
			
			ss.writeAdditionData(stream);
		}
		return ss;
		
	}
	
	private void doAdditionSnapshot() {
		if(this.streams==null || this.streams.size()==0)
			return ;
		
		boolean locked = this.lockSnapshot(true);
		if(locked) {
			try {
				logger.info("start to do addition snapshot, group={}",this.idx);
				Snapshot ss = new Snapshot();
				ss.setTime(new Date());
				ss.setIndex(0);
				ss.setType(Snapshot.Type.addition);
				ss.setParent(null);
				ss.setGroupIdx(this.idx);
				
				for(DataPointStream stream :this.streams.values()) {
					Metric metric = stream.getMetric();
					this.lockStream(metric.getId());
					try {
						ss = this.writeAdditionSnapshot(ss, stream);
					}catch(Exception e) {
						logger.error("failed to do snapshot of metric:{}",metric,e);
					}finally {
						this.unlockStream(metric.getId());
					}
				}
				if(ss.getData()!=null && ss.getData().length>0) {
					this.snapshotDAO.create(ss);
				}
				logger.info("finish to do addition snapshot,group={}",this.idx);
			}catch(Exception e) {
				logger.error("执行增量快照过程中产生异常",e);
			}finally {
				this.unloackSnapshot();
				preSnapshotAt = System.currentTimeMillis();
			}
		}
	}
	/**
	 * 全量快照过程中，如果有新的时序数据从mainStream切换到extStream，则extStream中的内容不会被快照记录。
	 * 一旦有新的extStream用于记录数据，系统在几秒内就会执行归档，并重新执行全量快照。虽然是很短的窗口期，
	 * 但如果在这个窗口期内发生宕机事故，可能会丢失几秒钟的数据
	 */
	
	private void doFullSnapshot() {		
		ObjectId rootSnapshotId = null;
		boolean locked = this.lockSnapshot(false);
		if(locked) {
			try {
				logger.info("开始全量快照 ,group idx={}",this.idx);
				Snapshot ss = new Snapshot();
				ss.setTime(new Date());
				ss.setIndex(0);
				ss.setType(Snapshot.Type.full);
				ss.setParent(null);
				ss.setGroupIdx(this.idx);
				
				Set<Entry<Integer, DataPointStream>> set = this.streams.entrySet();
				for(Entry<Integer,DataPointStream> entry:set) {
					Integer key = entry.getKey();
					if(key>0) {
						int restSize = ss.restSize();
						DataPointStream stream = entry.getValue();
						this.lockStream(stream.getMetric().getId());
						try {
							int len = stream.getPointer();
							if(len>restSize){
								//剩下的空间不足
								Snapshot saved = snapshotDAO.create(ss);
								//写全量快照比较占用IO资源和网络资源，所以要sleep会儿
								Thread.sleep(20);
								if(rootSnapshotId==null)
									rootSnapshotId = saved.getId();
								ss = new Snapshot();
								Snapshot newPart = new Snapshot();
								newPart.setTime(saved.getTime());
								newPart.setIndex(saved.getIndex());
								newPart.setGroupIdx(this.idx);
								newPart.setType(Snapshot.Type.part);
								newPart.setParent(saved.getParent()==null?saved.getId():saved.getParent());
								ss = newPart;
							}
							ss.writeFullData(stream);
						}catch(Exception e) {
							logger.error("读取DataPointStream，并写全量快照期间产生错误",e);
						}finally {
							this.unlockStream(stream.getMetric().getId());
						}
					}
				}
				if(ss.getData()!=null && ss.getData().length>0) {
					Snapshot saved = snapshotDAO.create(ss);
					
					if(rootSnapshotId==null)
						rootSnapshotId = saved.getId();
				}
				logger.info("全量快照执行完毕！");
				
				//删除过期的snapshot
				if(rootSnapshotId!=null) {
					logger.info("删除过期快照，id<{}",rootSnapshotId);
					deleteTimeoutSnapshots( rootSnapshotId);
				}
			}catch(Exception e) {
				logger.error("执行全量快照期间产生错误!",e);
			}finally {
				//记录最近完成快照的时间
				preSnapshotAt =  System.currentTimeMillis();				
				this.unloackSnapshot();
			}
		}
	}
	
	private void deleteTimeoutSnapshots(ObjectId lastFullSnapshotId) {
		Timer timer = new Timer() ;
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				List<ObjectId> ids = snapshotDAO.findIdsBefore(idx, 
					lastFullSnapshotId,snapshotCleanBatchSize);
				if(ids!=null && ids.size()>0)
					snapshotDAO.delete(ids);
				else {
					timer.cancel();
					logger.info("过期快照清理完毕！");
				}
			}
		}, 500, this.snapshotCleanInterval);
		
	}
}
