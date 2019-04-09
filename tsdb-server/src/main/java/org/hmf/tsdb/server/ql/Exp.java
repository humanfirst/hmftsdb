package org.hmf.tsdb.server.ql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.DownSample;
import org.hmf.tsdb.server.service.DataPointService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 举例:
 * avg(温度{type="室内环境温度",area="机房1"|"机房2"}) by room 该查询同时获得机房1所有温度计求平均合并后的曲线和机房2所有温度求平均合并后的曲线
 * max(温度{type="室内环境温度"}) 所有机房最大室内温度曲线
 * 
 * sub(max(温度{type="室内温度"}),min(温度{type="室内温度"})) by room 该查询获得所有房间温度差曲线
 * sub(max(温度{type="室内温度",room="房间1"}),min(温度{type="室内温度",room="房间1"}))
     或sub(max(温度),min(温度)){type="室内温度",room="房间1"}
     
   
 * mult(电压{},电流{}) by device 求所有设备电压和电流的积(即功率)。
 * div(电压{},电流{}) 求电阻变化曲线
 */
public abstract class Exp {
	private static Logger logger = LoggerFactory.getLogger(Exp.class);
	protected Date from;
	protected Date to;
	protected Exp parent;
	protected List<Exp> children =  new ArrayList<Exp>();
	protected List<DataPointSerial> result = null;
	protected DownSample ds = null;
	
	public List<DataPointSerial> getResult() {
		return result;
	}

	public void setResult(List<DataPointSerial> result) {
		this.result = result;
	}

	public Date getFrom() {
		return from;
	}
	public void setFrom(Date from) {
		this.from = from;
	}
	public Date getTo() {
		return to;
	}
	public void setTo(Date end) {
		this.to = end;
	}
	public Exp getParent() {
		return parent;
	}
	public void setParent(Exp parent) {
		this.parent = parent;
	}
	public List<Exp> getChildren() {
		return children;
	}
	public void setChildren(List<Exp> children) {
		this.children = children;
	}
	public void addChild(Exp exp) {
		this.children.add(exp);
	}
	
	
	public DownSample getDs() {
		return ds;
	}

	public void setDs(DownSample ds) {
		this.ds = ds;
	}

	public abstract List<DataPointSerial> execSelf(DataPointService service) throws Exception;
	
	public List<DataPointSerial> exec(DataPointService service)throws Exception{
		List<DataPointSerial> result = null;
		execChildren(service);
		result = execSelf(service);
		this.result = result;
		return result;
	}
	
	protected long getLastTime() {
		long last = new Date().getTime();
		if(this.children!=null && this.children.size()>0) {
			for(Exp child:this.children) {
				if(child.result!=null && child.result.size()>0) {
					for(DataPointSerial dps:child.result) {
						if(dps.getEnd()>last)
							last = dps.getEnd();
					}
				}
			}
		}
		return last;
	}
	
	private void execChildren(DataPointService service)throws Exception{
		if(this.children!=null && this.children.size()>0) {
			children.parallelStream().forEach(exp->{
				try {
					exp.exec(service);
				} catch (Exception e) {
					logger.error("内嵌表达式执行失败！",e);
				}
			});
		}
	}
	
	public Exp findRoot() {
		Exp parent = null;
		Exp current = this;
		while((parent=current.parent)!=null) {
			current = parent;
		}
		return current;
	}
	
	public static void main(String[] args) throws Exception {
		
	}

	
}
