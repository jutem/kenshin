package com.kenshin.search.core.util;

import java.util.Date;
import java.util.UUID;


public class CommonUtil {
	
	public static String formactDate(long time) {
		return Constants.DATEFORMAT.format(new Date(time));
	}
	
	public static long getUniqueId() {
		return Math.abs(UUID.randomUUID().getLeastSignificantBits());
	}
}
