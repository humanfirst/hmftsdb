package org.hmf.tsdb.server.service.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.bson.conversions.Bson;
import org.hmf.tsdb.DataPoint;
import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.DownSample;
import org.hmf.tsdb.DownSample.Arithmetic;
import org.hmf.tsdb.server.dao.DpsInDayDAO;
import org.hmf.tsdb.server.dao.DpsInHourDAO;
import org.hmf.tsdb.server.entity.DpsInDay;
import org.hmf.tsdb.server.entity.DpsInHour;
import org.hmf.tsdb.server.entity.Metric;
import org.hmf.tsdb.server.entity.Summary;
import org.hmf.tsdb.server.history.DataPointStream;
import org.hmf.tsdb.server.history.DataPointStreamGroup;
import org.hmf.tsdb.server.ql.Compiler;
import org.hmf.tsdb.server.ql.Exp;
import org.hmf.tsdb.server.ql.SelectorExp;
import org.hmf.tsdb.server.ql.TagExp;
import org.hmf.tsdb.server.service.DataPointService;
import org.hmf.tsdb.server.service.MetricService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.mongodb.client.model.Filters;

@Service
public class DataPointServiceImpl implements DataPointService,ApplicationListener<ContextRefreshedEvent>{
	private static Logger logger = LoggerFactory.getLogger(DataPointServiceImpl.class);
	@Autowired
	private MetricService metricService = null;
	
	
	private DataPointStreamGroup streamGroup = null;
	
	
	@Autowired
	private DpsInHourDAO dpsInHourDAO = null;
	
	@Autowired
	private DpsInDayDAO dpsInDayDAO = null;
	
	
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if(streamGroup==null) {
			streamGroup = (DataPointStreamGroup) event.getApplicationContext().getBean(DataPointStreamGroup.class);
			streamGroup.init();
		}
	}
	
	@PreDestroy
	public void destory() {
		this.streamGroup.destory();
	}
	
	@Override
	public void acceptDataPoint(DataPoint dp) {
		streamGroup.acceptDataPoint(dp);
	}
	
	@Override
	public void acceptDataPointList(List<DataPoint> dps) {
		streamGroup.acceptDataPointList(dps);
	}	
	
	private DpsInHour getDpsInHourFromStream(Metric metric,DownSample sample) {
		DataPointStream stream = this.streamGroup.getStream(metric);
		if(stream.getPointer()>0) {
			DpsInHour one = new DpsInHour(stream);
			doSample(sample,one);
			return one;
		}else {
			return null;
		}
	}
	private DpsInHour getDpsInHourFromExtStream(Metric metric,DownSample sample) {
		DataPointStream stream = this.streamGroup.getExtStream(metric);
		if(stream.getPointer()>0) {
			DpsInHour one = new DpsInHour(stream);
			doSample(sample,one);
			return one;
		}else {
			return null;
		}
	}
	
	/**
	 * 本次降频采样是 不完全的降频:仅仅在不需要detail的时候剔除细节数据，保留概要数据
	 * @param sample
	 * @param dps
	 */
	private void doSample(DownSample sample, DpsInHour dps) {
		if(sample!=null){
			if(sample.getUnit().equals(TimeUnit.HOURS) || sample.getUnit().equals(TimeUnit.DAYS)){
				dps.doSummary();
				dps.getDps().clear();
				dps.setDps(null);
			}
		}
	}
	
	private Map<Metric,List<DpsInHour>> findDpsFromMem(Set<Metric> metrics,Date start,Date end,DownSample sample){
		Map<Metric,List<DpsInHour>> map = new HashMap<Metric,List<DpsInHour>>();
		for(Metric metric:metrics) {			
			List<DpsInHour> list = new ArrayList<DpsInHour>();
			DpsInHour dpsOfCurrentHour = this.getDpsInHourFromStream(metric,sample);
			DpsInHour dpsOfNextHour = this.getDpsInHourFromExtStream(metric,sample);
			if(dpsOfCurrentHour!=null)
				list.add(dpsOfCurrentHour);
			if(dpsOfNextHour!=null)
				list.add(dpsOfNextHour);
			
			if(list!=null && list.size()>0)
				map.put(metric, list);
		}
		return map;
	}
	
	private void merge(Map<Metric,List<DpsInHour>> target ,Map<Metric,List<DpsInHour>> from){
		Set<Entry<Metric,List<DpsInHour>>> entries = from.entrySet();
		for(Entry<Metric,List<DpsInHour>> entry:entries) {
			Metric metric = entry.getKey();
			List<DpsInHour> set = target.get(metric);
			if(set==null) {
				set = new ArrayList<DpsInHour>();
				target.put(metric, set);
			}
			List<DpsInHour> dpsList = entry.getValue();
			if(dpsList!=null)
				set.addAll(dpsList);
		}
	}
	
	private long getZeroOfToday() {
		Calendar today = Calendar.getInstance();
		today.set(Calendar.HOUR_OF_DAY, 0);
		today.set(Calendar.MINUTE,0);
		today.set(Calendar.SECOND,0);
		today.set(Calendar.MILLISECOND,0);
		return today.getTimeInMillis();
	}
	
	private long getZeroOfDay(Date day) {
		Calendar c = Calendar.getInstance();
		c.setTime(day);
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE,0);
		c.set(Calendar.SECOND,0);
		c.set(Calendar.MILLISECOND,0);
		return c.getTimeInMillis();
	}
	
	private List<DataPointSerial> queryByMetrics(Set<Metric> metrics, Date start, Date end, DownSample sample){
		List<DataPointSerial> result = new ArrayList<DataPointSerial>();
		long t0,t1;
		
		//从按天归档的表查询
		t0 = System.currentTimeMillis();
		long zeroOfToday = this.getZeroOfToday();
		Map<Metric,List<DpsInHour>> dpsFromDayPack = null;
		if(start.getTime()<zeroOfToday) {
			dpsFromDayPack = this.dpsInDayDAO.findDpsPack(metrics, start, end, sample);
		}
		if(dpsFromDayPack==null)
			dpsFromDayPack = new HashMap<Metric,List<DpsInHour>>();
		t1 = System.currentTimeMillis();
		logger.info("从dpsInDay表获取{}个dps,耗时{}ms",dpsFromDayPack.size(),(t1-t0));	
		
		//从按小时归档的表查询
		t0 = System.currentTimeMillis();
		Map<Metric,List<DpsInHour>> dpsFromHourPack = null;
		dpsFromHourPack = this.dpsInHourDAO.findSerials(metrics, start, end,sample);
		t1 = System.currentTimeMillis();
		logger.info("从dps表获取{}个dps,耗时{}ms",dpsFromHourPack.size(),(t1-t0));
		
		
		//从内存查询
		t0 = System.currentTimeMillis();
		Map<Metric, List<DpsInHour>> dpsFromMem = null;
		try {
			dpsFromMem = this.findDpsFromMem(metrics, start, end, sample);
		} catch (Exception e) {
			logger.error("failed to find dps from other node",e);
		}
		t1 = System.currentTimeMillis();
		logger.info("从内存获取{}个dps,耗时{}ms",dpsFromMem.size(),(t1-t0));
		
		
		//合并
		Map<Metric,List<DpsInHour>> merged = new LinkedHashMap<Metric,List<DpsInHour>>();
		this.merge(merged, dpsFromDayPack);
		this.merge(merged, dpsFromHourPack);
		this.merge(merged, dpsFromMem);
		long now = System.currentTimeMillis();
		Set<Entry<Metric,List<DpsInHour>>> entries = merged.entrySet();
		for(Entry<Metric,List<DpsInHour>> entry:entries) {
			Metric metric = entry.getKey();
			List<DpsInHour> dpsSet = entry.getValue();
			this.removeDpsOverRange(dpsSet, start, end);
			DataPointSerial dps = this.downSample(dpsSet, sample);
			dps.setStart(start.getTime());
			dps.setEnd(end==null?now:end.getTime());
			dps.setMetric(metric.getName());
			dps.setTags(metric.getTags());
			result.add(dps);
		}
		return result;
	}
	
	private void removeDpsOverRange(List<DpsInHour> list,Date start,Date end) {
		if(list!=null && list.size()>0){			
			list.get(0).subDps(start, end);
			if(list.size()>1){
				list.get(list.size()-1).subDps(start, end);
			}
		}
	}
	
	private DataPointSerial downSample(List<DpsInHour> dpsInHours,DownSample sample){
		DataPointSerial dps = new DataPointSerial();
		if(sample==null){
			for(DpsInHour dpsInHour:dpsInHours){
				long hour = dpsInHour.getHour();
				long zeroSecondOfHour = hour* 3600;
				Iterator<Entry<Integer,Number>> iter = dpsInHour.getDps().entrySet().iterator();
				while(iter.hasNext()){
					Entry<Integer,Number> entry = iter.next();
					long second = zeroSecondOfHour + entry.getKey();
					dps.addDataPoint(second*1000, entry.getValue());
				}
			}
		}else{
			TimeUnit unit = sample.getUnit();
			if(unit.equals(TimeUnit.MICROSECONDS) 
					|| unit.equals(TimeUnit.MILLISECONDS)
					|| unit.equals(TimeUnit.NANOSECONDS)
					|| unit.equals(TimeUnit.SECONDS))
				throw new RuntimeException("can not support downsample on this time unit:"+unit.toString());
			
			
			if(unit.equals(TimeUnit.HOURS) || unit.equals(TimeUnit.DAYS)){
				//从小时摘要数据降频采样
				Map<String,Number> sampled = sampleFromSummary(dpsInHours,sample);
				dps.setValues(sampled);
			}else{
				//从小时详细数据降频采样
				Map<String,Number> sampled = sampleFromDetail(dpsInHours,sample);
				dps.setValues(sampled);
			}
		}	
		return dps;
	}
	
	private Number sampleFromSummary(Summary summary,DownSample sample){
		Arithmetic arithmetic = sample.getArithmetics().get(0);
		if(arithmetic.equals(Arithmetic.avg))
			return summary.getAvg();
		else if(arithmetic.equals(Arithmetic.min))
			return summary.getMin();
		else if(arithmetic.equals(Arithmetic.max))
			return summary.getMax();
		else if(arithmetic.equals(Arithmetic.first))
			return summary.getBegin();
		else if(arithmetic.equals(Arithmetic.last))
			return summary.getEnd();
		else
			throw new RuntimeException("can not to execute sample Arithmetic:"+sample.getArithmetics().get(0).toString());
	}
	
	private Map<String,Number> sampleFromSummary(List<DpsInHour> dpsInHours,DownSample sample){
		int frequenceLen = changeTimeLenToHours(sample.getFrequence(),sample.getUnit());
		long sampleStartAt = 0;
		Map<String,Number> result = new LinkedHashMap<String,Number>();
		Map<Long,Number> subList = new LinkedHashMap<Long,Number>();
		
		for(DpsInHour dpsInHour:dpsInHours){			
			long hour = dpsInHour.getHour();	
			if(sampleStartAt==0){
				sampleStartAt = hour;
			}
			if(dpsInHour.getSummary()==null){
				continue;
			}
			Number value = this.sampleFromSummary(dpsInHour.getSummary(), sample);
			
			if(hour-sampleStartAt+1<=frequenceLen){
				//把 同一采样周期的数据放入集合
				subList.put(hour,value);				
			}else{
				//对前一个采样周期的数据进行采样
				if(subList.size()>0){
					Number aggregated = doSample(subList,sample);
					result.put(Long.toString(sampleStartAt*3600*1000), aggregated);
				}
				//开始新的采样周期：清除前一个采样周期产生的集合数据，重置采样开始时间
				subList.clear();
				subList.put(hour,value);
				sampleStartAt = hour;
			}
		}
		if(subList.size()>0){
			Number aggregated = doSample(subList,sample);
			result.put(Long.toString(sampleStartAt*3600*1000), aggregated);
		}
		return result;
	}
	
	/**
	 * 从原始数据降频采样
	 * @param dpsInHours
	 * @param sample
	 * @return
	 */
	private Map<String,Number> sampleFromDetail(List<DpsInHour> dpsInHours,DownSample sample){
		int frequenceLen = changeTimeLenToSeconds(sample.getFrequence(),sample.getUnit());
		long sampleStartAt = 0;
		Map<String,Number> result = new LinkedHashMap<String,Number>();
		Map<Long,Number> subList = new LinkedHashMap<Long,Number>();
		for(DpsInHour dpsInHour:dpsInHours){			
			long hour = dpsInHour.getHour();
			long second = hour*3600;
			
			Iterator<Entry<Integer,Number>> iter = dpsInHour.getDps().entrySet().iterator();
			while(iter.hasNext()){
				Entry<Integer,Number> entry = iter.next();
				int secondInHour = entry.getKey();
				long time = second + secondInHour;
				if(sampleStartAt==0){
					sampleStartAt = frequenceLen *( time/frequenceLen);
				}
				Number value = entry.getValue();
				if(time-sampleStartAt+1<=frequenceLen) 
					//把同一个采样周期的数据放入集合
					subList.put(time, value);
				else{
					//跨入新采样周期之前，先对前一个周期进行采样
					Number aggregated = doSample(subList,sample);
					result.put(Long.toString(sampleStartAt*1000), aggregated);
					//开始新的采样周期：清除前一个采样周期产生的集合数据，重置采样开始时间
					subList.clear();
					subList.put(time, value);
					sampleStartAt = frequenceLen *( time/frequenceLen);
				}
			}
			if(subList.size()>0){
				Number aggregated = doSample(subList,sample);
				result.put(Long.toString(sampleStartAt*1000), aggregated);
			}
		}
		return result;
	}
	
	private Number doSample(Map<Long,Number> values,DownSample sample){
		Arithmetic arithmetic = sample.getArithmetics().get(0);
		if(arithmetic.equals(Arithmetic.max)){
			Optional<Entry<Long,Number>> max = values.entrySet().stream().max(new NumberComparator());
			return max.get().getValue();
		}else if(arithmetic.equals(Arithmetic.min)){
			Optional<Entry<Long,Number>> min = values.entrySet().stream().min(new NumberComparator());
			return min.get().getValue();
		}else if(arithmetic.equals(Arithmetic.avg)){
			return this.avg(values);
		}else if(arithmetic.equals(Arithmetic.first)){
			Number first = null; 
			Iterator<Entry<Long,Number>> iter = values.entrySet().iterator();
			while(iter.hasNext()){
				Number c =  iter.next().getValue();
				if(c!=null){
					first = c;
					break;
				}
			}
			return first;
		}else if(arithmetic.equals(Arithmetic.last)){
			Number last = null; 
			Iterator<Entry<Long,Number>> iter = values.entrySet().iterator();
			while(iter.hasNext()){
				Number c =  iter.next().getValue();
				if(c!=null)
					last = c;
			}
			return last;
		}else {
			throw new RuntimeException("not yet support this sample Arithmetic:"+arithmetic.toString()) ;
		}
	}

	@Override
	public List<DataPointSerial> queryBySelector(SelectorExp selector) throws Exception {
		String metricName = selector.getMetric();
		List<TagExp> tags = selector.getTags();
		Bson query = this.buildQuery(metricName, tags);
		List<Metric> list = this.metricService.findMetricsBy(query);
		Set<Metric> metrics = new HashSet<Metric>();
		metrics.addAll(list);
		if(metrics.size()>100) {
			throw new Exception("本次查询需要汇聚"+metrics.size()+"个测点的时序数据,超过了系统的限制");
		}else {
			Exp root = selector.findRoot();
			Date start = root.getFrom();
			Date end = root.getTo();
			DownSample sample = root.getDs();
			return this.queryByMetrics(metrics, start, end, sample);
		}
	}
	@Override
	public List<DataPointSerial> queryByExp(Exp exp)throws Exception{
		if(exp.getFrom()==null)
			throw new Exception("表达式必须包含开始时间！");
		return exp.exec(this);
	}
	@Override
	public List<DataPointSerial> queryByExp(String exp)throws Exception{
		Exp e = Compiler.compile(exp);
		return queryByExp(e);
	}
	
	private Bson buildFilter(TagExp tag) {
		if(tag.getValue()==null || tag.getValue().size()==0)
			throw new RuntimeException("tag过滤器的值不可以为空");
		Bson filter = null;
		String symbol = tag.getSymbol();
		if(symbol.equals("=")) {
			if(tag.getValue().size()==1) {
				filter = Filters.eq("tags."+tag.getName(),tag.getValue().get(0));
			}else{
				filter = Filters.in("tags."+tag.getName(),tag.getValue());
			}
		}else if(symbol.equals("!=")) {
			if(tag.getValue().size()==1) {
				filter = Filters.ne("tags."+tag.getName(),tag.getValue().get(0));
			}else{
				filter = Filters.nin("tags."+tag.getName(),tag.getValue());
			}
		}else {
			throw new RuntimeException("tag过滤器不支持这种符号:"+tag.getSymbol());
		}
		return filter;
	}
	private Bson buildQuery(String metric,List<TagExp> tags) {
		Bson metricFilter = Filters.eq("name",metric);
		Bson query = null;
		if(tags!=null | tags.size()>0) {
			List<Bson> filters = new ArrayList<Bson>();
			filters.add(metricFilter);
			for(TagExp tag:tags) {
				Bson filter = buildFilter(tag);
				filters.add(filter);
			}
			query = Filters.and(filters);			
		}else {
			query = metricFilter;
		}
		return query;
	}
	
	class NumberComparator implements Comparator<Entry<Long,Number>>{
		public int compare(Entry<Long,Number> e1, Entry<Long,Number> e2) {
			Number number1 = e1.getValue();
			Number number2 = e2.getValue();
			if(number1 instanceof Float){
				Float f1 = (Float)number1;
				Float f2 = (Float)number2;
				return f1.compareTo(f2);
			}else if(number2 instanceof Integer){
				Integer i1 = (Integer)number1;
				Integer i2 = (Integer)number2;
				return i1.compareTo(i2);
			}else{
				throw new RuntimeException("can not compare these numbers:"
						+number1.getClass()+","+number2.getClass());
			}
		}
	}
	
	private Number avg(Map<Long,Number> values){
		Iterator<Entry<Long,Number>> iter = values.entrySet().iterator();
		Class<?> type = null;
		while(iter.hasNext()){
			Entry<Long,Number> entry = iter.next();
			Number n = entry.getValue();
			if(n instanceof Float ){
				type = Float.class;
				break;
			}else if (n instanceof Integer){
				type = Integer.class;
				break;
			}else {
				logger.error("存在不可识别的类型：{},",values);
				throw new RuntimeException("can not calculate avg value on unknow type,time={"+entry.getKey()+"},value={"+((Double)n)+"}");
			}
		}
		
		if(type.equals(Float.class)){
			return avgFloat(values);
		}else{
			return avgInt(values);
		}
			
	}
	
	private int avgInt(Map<Long,Number> values){
		int count = 0;
		int sum = 0;
		Set<Entry<Long,Number>> set = values.entrySet();
		for(Entry<Long,Number> e:set){
			Integer v = (Integer) e.getValue();
			sum = sum+v;
			count++;
		}
		return sum/count;
	}
	
	private float avgFloat(Map<Long,Number> values){
		int count = 0;
		float sum = 0;
		Set<Entry<Long,Number>> set = values.entrySet();
		for(Entry<Long,Number> e : set){
			Float f = (Float)e.getValue();
			sum = sum+ f.floatValue();
			count++;
		}
		return sum/count;
	}
	
	private int changeTimeLenToSeconds(int len,TimeUnit unit ){
		if(unit.equals(TimeUnit.DAYS)){
			return len * 24 * 3600;
		}else if(unit.equals(TimeUnit.HOURS)){
			return len * 3600;
		}else if(unit.equals(TimeUnit.MINUTES)){
			return len * 60;
		}else
			throw new RuntimeException("can not change time len to second with unit is "+ unit.toString());
	}
	
	private int changeTimeLenToHours(int len,TimeUnit unit ){
		if(unit.equals(TimeUnit.DAYS)){
			return len * 24 ;
		}else if(unit.equals(TimeUnit.HOURS)){
			return len ;
		}else
			throw new RuntimeException("can not change time len to second with unit is "+ unit.toString());
	}

	
	@Scheduled(cron="0 5 0,1,2,3,4,5,6 * * ? ")
	public void doFileInDay() {
		logger.info("按天归档开始");
		Map<Integer,Metric> metricsMap = metricService.getMetrics();
		Collection<Metric> metrics= metricsMap.values();
		for(Metric metric:metrics) {
			this.calcAndFileDpsInDay(metric.getId());
		}
		logger.info("按天归档结束");
	}

	private void setCalendarToZeroClock(Calendar c) {
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.MILLISECOND, 0);
		c.set(Calendar.SECOND, 0);
	}
	private int calcAndFileDpsInDay(Integer metricId) {
		int count = 0;
		Date startTime = null;
		//找到第一个未按天归档的DpsInDay所在时间
		startTime = this.dpsInHourDAO.findFirstTimestamp(metricId);
		if(startTime==null)
			return 0;
		
		//把时间设定为所在日的零点
		Calendar currentDay = Calendar.getInstance();
		currentDay.setTime(startTime);
		setCalendarToZeroClock(currentDay);
		
		Calendar today = Calendar.getInstance();
		this.setCalendarToZeroClock(today);
		
		while(currentDay.before(today)) {
			long startSecond = currentDay.getTimeInMillis()/1000;
			long endSecond = startSecond+3600*24-1;
			//查询是否存在此后一天的数据
			boolean existNextDayDps = this.dpsInHourDAO.exitsDpsAfterSecond(metricId,endSecond);
			if(!existNextDayDps) {
				//没有后一天的数据，不能判断currentDay的数据是否已经采集完整
				break;
			}
			
			//查找currentDay这天的所有dpsInHour
			List<DpsInHour> list = this.dpsInHourDAO.findDpsPack(metricId, new Date(startSecond*1000), new Date(endSecond*1000), null);
			
			//把currentDay这天的dpsInHour合并到dpsInDay，并保存
			Map<String,Object> data = new LinkedHashMap<String,Object>();
			DpsInDay dpsInDay = new DpsInDay(currentDay.getTime(),metricId,data);
			for(DpsInHour dps:list) {
				long time = dps.getHour()*3600000;
				Calendar c = Calendar.getInstance();
				c.setTimeInMillis(time);
				int hour = c.get(Calendar.HOUR_OF_DAY);
				Summary summary = dps.getSummary();
				byte[] bytes = dps.getData();
				data.put("summary_"+hour, summary);
				data.put("detail_"+hour, bytes);
			}
			try {
				if(this.dpsInDayDAO.exits(dpsInDay.getId())==false)
					dpsInDayDAO.create(dpsInDay);
			}catch(Exception e) {
				logger.error("failed to create dpsInDay record",e);
			}
			//删除currentDay这天内按小时归档的数据
			Date nextDay = new Date((endSecond+1)*1000);
			dpsInHourDAO.removeDpsBefore(metricId,nextDay);
			currentDay.add(Calendar.DATE, 1);
			count++;
		}
		return count;
	}
	
}
