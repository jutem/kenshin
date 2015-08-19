package com.kenshin.search.core.util;

import java.util.Date;


public class CommonUtil {
	
	public static String formactDate(long time) {
		return Constants.DATEFORMAT.format(new Date(time));
	}
}
