package org.hmf.tsdb.server.controller;


import java.util.List;

import org.hmf.tsdb.DataPointSerial;
import org.hmf.tsdb.Response;
import org.hmf.tsdb.server.dao.MetricDAO;
import org.hmf.tsdb.server.service.DataPointService;
import org.hmf.tsdb.server.service.MetricService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/query")
public class QueryController {
	static Logger logger = LoggerFactory.getLogger(QueryController.class);
	@Autowired
	DataPointService service;
	
	@Autowired
	MetricService metricService;
	
	@Autowired
	MetricDAO metricDAO = null;
	
	
	@RequestMapping("/findHistoryData")
	@ResponseBody
	public Object queryHistoryData(String query) {
		Response<List<DataPointSerial>> response = null;
		try {
			List<DataPointSerial> dps = service.queryByExp(query);
			response = Response.success(dps);
		}catch(Exception e) {
			logger.error("",e);
			response = Response.error(e.getMessage(), null);
		}
		return response;
	}
	
}	
