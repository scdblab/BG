package kosar;

import java.util.Set;

import kosar.AbstractDoDMLListener;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;


public class SimpleDoDMLListener extends AbstractDoDMLListener {

	private Set<String> iks;
	
	private DB db;
	
	public SimpleDoDMLListener(DB db, Set<String> iks) {
		super();
		this.iks = iks;
		this.db = db;
	}

	@Override
	public Set<String> getInternalKeys(String dml) {
		// TODO Auto-generated method stub
		return this.iks;
	}

	@Override
	public int updateDB(String dml) {
		if (this.db == null) {
			return 0;
		}
		String[] ems = dml.split(",");
		int inviter = Integer.parseInt(ems[1]);
		int invitee = Integer.parseInt(ems[2]);
		int val = -1;
		switch (dml.charAt(0)) {
		case CoreClient.INVITE:
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
		case CoreClient.ACCEPT:
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
		case CoreClient.REJECT:
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
		case CoreClient.THAW:
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

	@Override
	public void setDB(DB db) {
		this.db = db;
	}
}
