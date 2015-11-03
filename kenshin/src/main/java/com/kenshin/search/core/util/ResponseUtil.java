package com.kenshin.search.core.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;

public class ResponseUtil {

	private static final Logger logger = Logger.getLogger(ResponseUtil.class);

	/**
	 * 设置浏览器输出头
	 */
	public static void setCacheHeader(HttpServletResponse resp, long cacheTime) {
		if (cacheTime <= 0) {
			setNoCacheHeader(resp);
			return;
		}
		long now = new Date().getTime();
		resp.setDateHeader("Expires", now + cacheTime);
		resp.setDateHeader("Last-Modified", now);
		resp.setHeader("Cache-Control", "max-age=" + (cacheTime / 1000));
	}

	public static void setNoCacheHeader(HttpServletResponse resp) {
		resp.setHeader("Pragma", "No-Cache");
		resp.setHeader("Cache-Control", "no-cache, no-store");
		resp.setDateHeader("Expires", 0);
	}

	public static void writeStringResponse(HttpServletRequest req, HttpServletResponse resp, String html, long cacheTime) {
		writeResponse(req, resp, html, cacheTime);
	}

	public static void writeResponse(HttpServletRequest req, HttpServletResponse resp, String content, long cacheTime) {
		String charset = req.getParameter("charset");

		PrintWriter writer = null;
		try {
			resp.setHeader("P3P", "CP=CAO PSA OUR");
			if (StringUtils.isNotBlank(charset)) {
				resp.setCharacterEncoding(charset);
			} else {
				resp.setCharacterEncoding("UTF-8");
			}

			resp.setContentType("text/plain");
			setCacheHeader(resp, cacheTime);
			writer = resp.getWriter();
			writer.write(content);
		} catch (IOException e) {
			logger.error("writeResponse error:", e);
		} finally {
			IOUtils.closeQuietly(writer);
		}
	}
	
	public static void writeRightResponse(HttpServletRequest req, HttpServletResponse resp, Object obj, long cacheTime) {
		JSONObject jo = new JSONObject();
		jo.put("status", 0);
		jo.put("data", obj);
		writeResponse(req, resp, jo.toString(), cacheTime);
	}
}
