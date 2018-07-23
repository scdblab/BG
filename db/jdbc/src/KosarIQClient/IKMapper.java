package KosarIQClient;

import edu.usc.IQ.client.InternalKey2CoreMapper;

public class IKMapper implements InternalKey2CoreMapper {

	private final String[] coreAddr;
	
	public IKMapper(String[] coreAddr) {
		super();
		this.coreAddr = coreAddr;
	}

	@Override
	public String internalKey2Core(String internalKey) {
		int hash = (internalKey.hashCode() < 0 ? ((~internalKey.hashCode()) + 1) : internalKey
				.hashCode());
		int index = hash % this.coreAddr.length;
		return this.coreAddr[index];
	}

}
