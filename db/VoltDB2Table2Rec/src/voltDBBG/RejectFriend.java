package voltDBBG;

import org.voltdb.*;

public class RejectFriend extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "DELETE FROM PENDINGfriendship WHERE inviterid = ? AND inviteeid = ?;"
  );
  
  public VoltTable[] run( int inviterID , int inviteeID)
      throws VoltAbortException {
          voltQueueSQL( sql1, inviterID, inviteeID);
          voltExecuteSQL();
          return null;
      }
}
