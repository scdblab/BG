package kosar;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

/**
 * A factory helps to create packet for communicating between client and Core
 * 
 * @author Haoyu Huang
 *
 */
public class PacketFactory {

	public static final int CMD_REGISTER = 200;
	public static final int CMD_UNREGISTER = 201;

	public static final int CMD_REQUEST_REGISTER = -200;
	public static final int CMD_REQUEST_ABORTED = -100;

	public static final int CMD_RENEW_LIVE_LEASE = 10;
	public static final int CMD_LIVE_LEASE_RENEWED = 0;
	public static final int CMD_TEST_INVALIDATE = 1999;
	// request codes
	public static final int CMD_ACQUIRE_I_LEASE = 1;
	public static final int CMD_ACQUIRE_I_LEASE_REJECT = -10;
	public static final int CMD_RELEASE_I_LEASE = 2;
	public static final int CMD_STOLEN = 104;
	public static final int CMD_CONSUMED = 105;
	// response codes
	public static final int CMD_I_LEASE_GRANTED = 0;
	public static final int CMD_I_LEASE_REJECTED = -1;
	public static final int CMD_TRIGGER_REGISTER_FAILED = -2;
	public static final int CMD_STEAL = 101;
	public static final int CMD_COPY = 102;
	public static final int CMD_USE_ONCE = 103;

	public static final int CMD_USE_REJECTED = -105;
	public static final int CMD_USE_NULL = -101;
	public static final int CMD_USE_GRANTED = 0;

	public static final int CMD_I_LEASE_RELEASED = 0;
	public static final int CMD_STOLEN_RELEASED = 1;
	public static final int CMD_CONSUMED_RELEASED = 2;
	public static final int CMD_I_LEASE_REVOKED = -1;

	public static final int CMD_INVALIDATE_NOTIFICATION = 3;
	public static final int CMD_Q_LEASE_GRANTED = 0;
	public static final int CMD_Q_LEASE_REJECTED = -1;
	public static final int CMD_INVALIDATED = 0;
	public static final int CMD_INVALIDATE_FAIL = -1;

	public static final int CMD_COMMIT_TID = 4;
	public static final int CMD_ABORT_TID = 5;
	public static final int CMD_Q_LEASE_RELEASED = 0;
	public static final int CMD_Q_LEASE_ABORTED = -1;
	
	public static final int CMD_ACQUIRE_Q_LEASE_REFRESH = 6;
	public static final int CMD_RELEASE_Q_LEASE_REFRESH = 7;
	public static final int CMD_ABORT_Q_LEASE_REFRESH = 8;
	public static final int CMD_ACQUIRE_Q_LEASE_REFILL = 9;
	
	public static final int CMD_REQUEST_UPDATE = 9;

	public static final int CMD_STAT = 10000;
	public static final int CMD_REINIT = 20000;
	
	public static final int CMD_OK = 0;
	public static final int CMD_DML_SHIPPING = 11;

	public static String translate(int CMD_Id) {
		switch (CMD_Id) {
		case CMD_REGISTER:
			return "register";
		case CMD_UNREGISTER:
			return "unregister";
		case CMD_ACQUIRE_I_LEASE:
			return "acquire I lease";
		case CMD_RELEASE_I_LEASE:
			return "release I lease";
		case CMD_INVALIDATE_NOTIFICATION:
			return "invalidate notification";
		case CMD_COMMIT_TID:
			return "commit";
		case CMD_ABORT_TID:
			return "abort";
		}
		return "";
	}

	public static ByteBuffer createInvalidateResponsePacket(
			Set<String> internalKeys) {
		int capacity = 8;
		for (String ik : internalKeys) {
			capacity += 4 + ik.length();
		}
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		buffer.putInt(CMD_INVALIDATE_NOTIFICATION);
		buffer.putInt(internalKeys.size());
		for (String ik : internalKeys) {
			buffer.putInt(ik.length());
			buffer.put(ByteUtil.string2Byte(ik));
		}
		return buffer;
	}

	// public static ByteBuffer createTestInvalidateResponsePacket(int tid,
	// Set<String> internalKeys) {
	// int capacity = 8;
	// capacity += 4;
	// for (String ik : internalKeys) {
	// capacity += 4 + ik.length();
	// }
	// ByteBuffer buffer = ByteBuffer.allocate(capacity);
	// buffer.putInt(CMD_TEST_INVALIDATE);
	// buffer.putInt(tid);
	// buffer.putInt(internalKeys.size());
	// for (String ik : internalKeys) {
	// buffer.putInt(ik.length());
	// buffer.put(ByteUtil.string2Byte(ik));
	// }
	// return buffer;
	// }

	public static ByteBuffer createAcquireILeasedResponsePacket(int commandId,
			Integer leaseNumber, Map<Integer, String> clientIds) {
		ByteBuffer buffer = null;
		switch (commandId) {
		case CMD_I_LEASE_GRANTED:
			buffer = ByteBuffer.allocate(4 + 4);
			buffer.putInt(CMD_I_LEASE_GRANTED);
			buffer.putInt(leaseNumber.intValue());
			break;
		case CMD_I_LEASE_REJECTED:
			buffer = ByteBuffer.allocate(4);
			buffer.putInt(CMD_I_LEASE_REJECTED);
			break;
		case CMD_STEAL:
		case CMD_COPY:
		case CMD_USE_ONCE:
			int capacity = 8;
			if (clientIds != null) {
				Set<Integer> ids = clientIds.keySet();
				for (Integer id : ids) {
					capacity += 8 + clientIds.get(id).length();
				}
			}
			buffer = ByteBuffer.allocate(capacity);
			buffer.putInt(commandId);
			if (clientIds != null) {
				buffer.putInt(clientIds.size());
				Set<Integer> ids = clientIds.keySet();
				for (Integer cliendId : ids) {
					buffer.putInt(cliendId.intValue());
					String addr = clientIds.get(cliendId);
					buffer.putInt(addr.length());
					buffer.put(ByteUtil.string2Byte(addr));
				}
			}
			break;
		default:
			break;
		}
		return buffer;
	}

	public static ByteBuffer createCommandIdResposnePacket(int commandId) {
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(commandId);
		return buffer;
	}

	public static ByteBuffer createCommandIdResposnePacket(int commandId,
			int clientId) {
		ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putInt(commandId);
		buffer.putInt(clientId);
		return buffer;
	}

	public static ByteBuffer createCommandIdResposnePacket(int commandId,
			byte[] buf) {
		ByteBuffer buffer = ByteBuffer.allocate(8 + buf.length);
		buffer.putInt(commandId);
		buffer.putInt(buf.length);
		buffer.put(buf);
		return buffer;
	}

	public static ByteBuffer createRegisterResposnePacket(int commandId,
			int clientId, int timeout) {
		ByteBuffer buffer = ByteBuffer.allocate(12);
		buffer.putInt(commandId);
		buffer.putInt(clientId);
		buffer.putInt(timeout);
		return buffer;
	}

}
