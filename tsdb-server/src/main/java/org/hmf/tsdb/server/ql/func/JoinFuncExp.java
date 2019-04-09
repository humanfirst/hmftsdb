package org.hmf.tsdb.server.ql.func;

import java.util.ArrayList;
import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.server.service.DataPointService;

public class JoinFuncExp extends FuncExp{

	@Override
	public List<DataPointSerial> execSelf(DataPointService service) {
		List<DataPointSerial> r = new ArrayList<DataPointSerial>();
		if(this.children!=null && this.children.size()>0) {
			this.children.stream().forEach(c->r.addAll(c.getResult()));
		}
		return r;
	}

}
