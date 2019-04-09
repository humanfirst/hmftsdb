package org.hmf.tsdb.server.ql;

import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.service.DataPointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 时序选择器：metric{tag1=a|b|c,tag2=t,tag3!=tag}
 
 */
public class SelectorExp extends Exp {
	private static Logger logger = LoggerFactory.getLogger(SelectorExp.class);
	private String metric;
	private List<TagExp> tags = new ArrayList<TagExp>();
	
	public String getMetric() {
		return metric;
	}


	public void setMetric(String metric) {
		this.metric = metric;
	}



	public List<TagExp> getTags() {
		return tags;
	}



	public void setTags(List<TagExp> tags) {
		this.tags = tags;
	}

	public void addTag(TagExp tag) {
		this.tags.add(tag);
	}
	


	

	@Override
	public String toString() {
		return "SelectorExp [metric=" + metric + ", tags=" + tags + ", from=" + from + ", to=" + to + ", children="
				+ children + ", result=" + result + ", ds=" + ds + "]";
	}


	@Override
	public List<DataPointSerial> execSelf(DataPointService service) throws Exception {		
		List<DataPointSerial> result = service.queryBySelector(this);
		return result;		
	}


}
