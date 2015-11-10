package com.kenshin.search.core.reader;

import com.kenshin.search.core.reader.manager.ReaderManager;
import com.kenshin.search.core.resource.DisruptorResourcePool;

public abstract class AbstractReader {
	
	protected final DisruptorResourcePool resourcePool;
	protected final ReaderManager readerManager;
	
	public AbstractReader(DisruptorResourcePool resourcePool,
			ReaderManager readerManager) {
		super();
		this.resourcePool = resourcePool;
		this.readerManager = readerManager;
	}
	
}
