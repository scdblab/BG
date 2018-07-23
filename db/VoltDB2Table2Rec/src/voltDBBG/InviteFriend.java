package voltDBBG;


import org.voltdb.*;

public class InviteFriend extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "INSERT INTO PENDINGfriendship values(?,?);"
  );
  
  public VoltTable[] run( int inviterID , int inviteeID)
      throws VoltAbortException {
          voltQueueSQL( sql1, inviterID, inviteeID);
          voltExecuteSQL();
          return null;
      }
}
