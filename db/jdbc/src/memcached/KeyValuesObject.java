package memcached;

/**
 * Represents a triple (k, v, v'), in which:
 * 	k is the key
 * 	v is the old value before changes are made
 * 	v' is the new value after changes
 * @author hieun
 *
 */
public class KeyValuesObject {
	private String key;
	private byte[] oldValue;
	private byte[] newValue;
	
	public KeyValuesObject(String key) {
		this.key = key;
		this.oldValue = null;
		this.newValue = null;
	}
	
	public KeyValuesObject(String key, byte[] oldValue, byte[] newValue) {
		this.key = key;
		this.oldValue = oldValue;
		this.newValue = newValue;
	}
	
	public String getKey() {
		return key;
	}
	
	public byte[] getOldValue() {
		return oldValue;
	}
	
	public byte[] getNewValue() {
		return newValue;
	}	
	
	public void setOldValue(byte[] value) {
		this.oldValue = value;
	}
	
	public void setNewValue(byte[] value) {
		this.newValue = value;
	}
}
