package KosarIQClient;

import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;


public class SimpleDoDMLListener {

	
	private DB db;
	
	public SimpleDoDMLListener(DB db) {
		super();
		this.db = db;
	}

	public int updateDB(String dml) {
		if (this.db == null) {
			return 0;
		}
		String[] ems = dml.split(",");
		int inviter = Integer.parseInt(ems[1]);
		int invitee = Integer.parseInt(ems[2]);
		int val = -1;
		switch (dml.charAt(0)) {
		case IQClientWrapper.INVITE:
			while (true) {
				val = this.db.inviteFriend(inviter, invitee);
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		case IQClientWrapper.ACCEPT:
			while (true) {
				val = this.db.acceptFriend(inviter, invitee);
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		case IQClientWrapper.REJECT:
			while (true) {
				val = this.db.rejectFriend(inviter, invitee);
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
		case IQClientWrapper.THAW:
			while (true) {
				val = this.db.thawFriendship(inviter, invitee);
				if (val == -3) {
					try {
						this.db.cleanup(true);
						while (!this.db.init());
					} catch (DBException e) {
						e.printStackTrace();
					}
				} else {
					break;
				}
			}
			break;
			default:
				System.out.println("no idead " + dml);
				break;
		}
		return val;
	}
	
	

}
