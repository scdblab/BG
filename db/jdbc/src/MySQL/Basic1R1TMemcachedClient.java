package MySQL;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.StringByteIterator;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;

public class Basic1R1TMemcachedClient extends DB {

	protected static final ObjectMapper MAPPER = new ObjectMapper();

	public static final String HOSTS_PROPERTY = "memcached.hosts";

	public static final int DEFAULT_PORT = 11211;

	public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY = "memcached.shutdownTimeoutMillis";
	public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";

	public static final String OBJECT_EXPIRATION_TIME_PROPERTY = "memcached.objectExpirationTime";
	public static final String DEFAULT_OBJECT_EXPIRATION_TIME = String.valueOf(Integer.MAX_VALUE);

	public static final String CHECK_OPERATION_STATUS_PROPERTY = "memcached.checkOperationStatus";
	public static final String CHECK_OPERATION_STATUS_DEFAULT = "true";

	public static final String READ_BUFFER_SIZE_PROPERTY = "memcached.readBufferSize";
	public static final String DEFAULT_READ_BUFFER_SIZE = "3000000";

	public static final String OP_TIMEOUT_PROPERTY = "memcached.opTimeoutMillis";
	public static final String DEFAULT_OP_TIMEOUT = "60000";

	public static final String FAILURE_MODE_PROPERTY = "memcached.failureMode";
	public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;

	public static final String PROTOCOL_PROPERTY = "memcached.protocol";
	public static final ConnectionFactoryBuilder.Protocol DEFAULT_PROTOCOL = ConnectionFactoryBuilder.Protocol.TEXT;
	public static final int SUCCESS = 0;
	public static final int NOT_FOUND = 1;
	public static final int FAIL = -1;

	private MemcachedClient memcachedClient;

	@Override
	public boolean init() throws DBException {
		try {
			memcachedClient = createMemcachedClient();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return super.init();
	}

	protected static void fromJson(String value, Vector<HashMap<String, ByteIterator>> result) throws IOException {
		String[] values = value.split(",");
		for (String val : values) {
			HashMap<String, ByteIterator> res = new HashMap<>();
			fromJson(val, res);
			result.add(res);
		}
	}

	protected static String toJson(Vector<HashMap<String, ByteIterator>> values) throws IOException {
		StringBuilder builder = new StringBuilder();
		for (Map<String, ByteIterator> val : values) {
			builder.append(toJson(val));
			builder.append(",");
		}
		if (builder.length() > 0) {
			builder.deleteCharAt(builder.length() - 1);
		}
		return builder.toString();
	}

	protected static void fromJson(String value, Map<String, ByteIterator> result) throws IOException {
		JsonNode json = MAPPER.readTree(value);
		for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields(); jsonFields.hasNext();) {
			Map.Entry<String, JsonNode> jsonField = jsonFields.next();
			String name = jsonField.getKey();
			JsonNode jsonValue = jsonField.getValue();
			if (jsonValue != null && !jsonValue.isNull()) {
				result.put(name, new StringByteIterator(jsonValue.asText()));
			}
		}
	}

	protected static String toJson(Map<String, ByteIterator> values) throws IOException {
		ObjectNode node = MAPPER.createObjectNode();
		Map<String, String> stringMap = StringByteIterator.getStringMap(values);
		for (Map.Entry<String, String> pair : stringMap.entrySet()) {
			node.put(pair.getKey(), pair.getValue());
		}
		JsonFactory jsonFactory = new JsonFactory();
		Writer writer = new StringWriter();
		JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
		MAPPER.writeTree(jsonGenerator, node);
		return writer.toString();
	}

	protected net.spy.memcached.MemcachedClient createMemcachedClient() throws Exception {
		ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();

		connectionFactoryBuilder.setReadBufferSize(
				Integer.parseInt(getProperties().getProperty(READ_BUFFER_SIZE_PROPERTY, DEFAULT_READ_BUFFER_SIZE)));

		connectionFactoryBuilder
				.setOpTimeout(Integer.parseInt(getProperties().getProperty(OP_TIMEOUT_PROPERTY, DEFAULT_OP_TIMEOUT)));

		String protocolString = getProperties().getProperty(PROTOCOL_PROPERTY);
		connectionFactoryBuilder.setProtocol(protocolString == null ? DEFAULT_PROTOCOL
				: ConnectionFactoryBuilder.Protocol.valueOf(protocolString.toUpperCase()));

		String failureString = getProperties().getProperty(FAILURE_MODE_PROPERTY);
		connectionFactoryBuilder.setFailureMode(
				failureString == null ? FAILURE_MODE_PROPERTY_DEFAULT : FailureMode.valueOf(failureString));

		// Note: this only works with IPv4 addresses due to its assumption of
		// ":" being the separator of hostname/IP and port; this is not the case
		// when dealing with IPv6 addresses.
		//
		// TODO(mbrukman): fix this.
		List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
		String[] hosts = getProperties().getProperty(HOSTS_PROPERTY).split(",");
		for (String address : hosts) {
			int colon = address.indexOf(":");
			int port = DEFAULT_PORT;
			String host = address;
			if (colon != -1) {
				port = Integer.parseInt(address.substring(colon + 1));
				host = address.substring(0, colon);
			}
			addresses.add(new InetSocketAddress(host, port));
		}
		return new net.spy.memcached.MemcachedClient(connectionFactoryBuilder.build(), addresses);
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		if (memcachedClient != null) {
			memcachedClient.shutdown(1000, MILLISECONDS);
		}
		super.cleanup(warmup);
	}

	@Override
	public void buildIndexes(Properties props) {
		// TODO Auto-generated method stub
		super.buildIndexes(props);
	}

	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		String key = "P" + profileOwnerID;
		try {
			GetFuture<Object> future = memcachedClient.asyncGet(key);
			Object document = future.get();
			if (document != null) {
				fromJson((String) document, result);
				return SUCCESS;
			}
			return NOT_FOUND;
		} catch (Exception e) {
			return FAIL;
		}
	}

	public int insertUserProfile(int profileOwnerID, HashMap<String, ByteIterator> result) {
		try {
			String key = "P" + profileOwnerID;
			OperationFuture<Boolean> future = memcachedClient.add(key, Integer.MAX_VALUE, toJson(result));
			if (future.getStatus().isSuccess()) {
				return SUCCESS;
			}
		} catch (Exception e) {
			return FAIL;
		}
		return FAIL;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		String key = "LF" + profileOwnerID;
		try {
			GetFuture<Object> future = memcachedClient.asyncGet(key);
			Object document = future.get();
			if (document != null) {
				fromJson((String) document, result);
				return SUCCESS;
			}
			return NOT_FOUND;
		} catch (Exception e) {
			return FAIL;
		}
	}

	public int insertFriends(int profileOwnerID, Vector<HashMap<String, ByteIterator>> result) {
		String key = "LF" + profileOwnerID;
		return insertResults(key, result);
	}

	public int insertPendingFriends(int profileOwnerID, Vector<HashMap<String, ByteIterator>> result) {
		String key = "LP" + profileOwnerID;
		return insertResults(key, result);
	}

	private int insertResults(String key, Vector<HashMap<String, ByteIterator>> result) {
		try {
			OperationFuture<Boolean> future = memcachedClient.add(key, Integer.MAX_VALUE, toJson(result));
			if (future.getStatus().isSuccess()) {
				return SUCCESS;
			}
		} catch (Exception e) {
			return FAIL;
		}
		return FAIL;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		String key = "LP" + profileOwnerID;
		try {
			GetFuture<Object> future = memcachedClient.asyncGet(key);
			Object document = future.get();
			if (document != null) {
				fromJson((String) document, results);
				return SUCCESS;
			}
			return NOT_FOUND;
		} catch (Exception e) {
			return FAIL;
		}
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		memcachedClient.delete("LF" + inviterID);
		memcachedClient.delete("LF" + inviteeID);
		memcachedClient.delete("LP" + inviteeID);
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		memcachedClient.delete("LP" + inviteeID);
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		memcachedClient.delete("LP" + inviteeID);
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		memcachedClient.delete("LF" + friendid1);
		memcachedClient.delete("LF" + friendid2);
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void createSchema(Properties props) {
		// TODO Auto-generated method stub

	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub
		return 0;
	}

}
