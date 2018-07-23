package common;

public class CacheClientConstants {

	/** The name of the property for the cache server host name. */
	public static final String CACHE_SERVER_HOST="cachehostname";
	
	/** The name of the property for the cache server port. */
	public static final String CACHE_SERVER_PORT="cacheport";
	
	/** Whether the client starts and stops the cache server. */
	public static final String MANAGE_CACHE_PROPERTY = "managecache";

	/** Whether the client starts and stops the cache server. */
	public static final String MANAGE_CACHE_PROPERTY_DEFAULT = "false";
		
	/** The name of the property for the TTL value. */
	public static final String TTL_VALUE="ttlvalue";
	
	/** The name of the property for the TTL value. */
	public static final String TTL_VALUE_DEFAULT="0";
	
	/** The name of the property for the cache server host name. */
	public static final String CACHE_SERVER_HOST_DEFAULT="127.0.0.1";
	
	/** The name of the property for the cache server port. */
	public static final String CACHE_SERVER_PORT_DEFAULT="4343";
	
	
	/** The name of the property for the listener server port. 
	 *  The listener is used to start up an instance of the cache. */
	public static final String LISTENER_PORT="listenerport";
	
	/** The name of the property for the listener server port. */
	public static final String LISTENER_PORT_DEFAULT="11111";
	
	/** The name of the property for enabling message compression. */
	public static final String ENABLE_COMPRESSION_PROPERTY="compress";
	
	/** The name of the property for enabling message compression. */
	public static final String ENABLE_COMPRESSION_PROPERTY_DEFAULT="false";
}
