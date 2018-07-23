package voltDBBG;


import org.voltdb.*;

public class GetPendingFriends extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT inviterid from PENDINGfriendship where inviteeid=?;"
  );

  public VoltTable[] run( int inviteeid)
      throws VoltAbortException {
          voltQueueSQL( sql1, inviteeid);
          return voltExecuteSQL();
      }
}
