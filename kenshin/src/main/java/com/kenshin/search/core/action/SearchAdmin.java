package com.kenshin.search.core.action;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.kenshin.search.core.util.ResponseUtil;

@Controller
public class SearchAdmin {

	/** 
	 * 心跳
	 * http://localhost:8088/kenshin/isLive.action
	 */
	@RequestMapping(value = "/isLive.action")
	public void isLive(HttpServletRequest request, HttpServletResponse response) {
		ResponseUtil.writeRightResponse(request, response, "live", 0);
	}

}
