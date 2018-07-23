package KosarIQClient;

import java.util.concurrent.ConcurrentHashMap;

import edu.usc.IQ.client.CacheListener;

public class CacheListenerImpl<V> implements CacheListener<V> {

	private ConcurrentHashMap<String, V>[] ikCache;

	public CacheListenerImpl(int size) {
		ikCache = (ConcurrentHashMap<String, V>[]) new ConcurrentHashMap[size];
		for (int i = 0; i < size; i++) {
			ikCache[i] = new ConcurrentHashMap<String, V>();
		}
	}
	
	@Override
	public V get(String key) {
		// TODO Auto-generated method stub
		return this.getCacheIndex(key).get(key);
	}

	@Override
	public void put(String key, V value, boolean cache) {
		if (cache) {
			this.getCacheIndex(key).put(key, value);
		}
	}

	public ConcurrentHashMap<String, V> getCacheIndex(String key) {
		int hash = (key.hashCode() < 0 ? ((~key.hashCode()) + 1) : key
				.hashCode());
		int index = hash % ikCache.length;
		return ikCache[index];
	}

	@Override
	public V remove(int arg0, long arg1, String core, String key) {
		return this.getCacheIndex(key).remove(key);
	}

}
