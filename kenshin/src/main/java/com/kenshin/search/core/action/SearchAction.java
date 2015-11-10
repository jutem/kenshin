package com.kenshin.search.core.action;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.kenshin.search.core.core.KenshiCore;
import com.kenshin.search.core.model.Model;

@Controller
public class SearchAction {

	private static final Logger logger = Logger.getLogger(SearchAction.class);
	
	@Resource
	private KenshiCore kenshinCore;
	
	
	/**
	 * http://localhost:8088/kenshin/index.action?modelName=helloworld
	 */
	@RequestMapping(value = "/index")
	public void index(HttpServletRequest req, HttpServletResponse resp,
			@RequestParam(value = "modelName", required = true) String modelName) {
		logger.debug("<<<<<<<<<<<<<<< test");
		Model model = new Model();
		model.setFile1(modelName);
//		resourcePool.pushOriginData(model);
		kenshinCore.getDisruptorResourcePool().getOriginProducer().onData(model);
	}
	
	/**
	 * http://localhost:8088/kenshin/query.action
	 */
	@RequestMapping(value = "/query")
	public void query(HttpServletRequest req, HttpServletResponse resp,
			@RequestParam(value = "uType", required = true) int uType) {
		
		
	}
	
}
