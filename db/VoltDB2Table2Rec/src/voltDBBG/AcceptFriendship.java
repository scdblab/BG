package voltDBBG;



import org.voltdb.*;

public class AcceptFriendship extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "INSERT INTO CONFIRMEDFRIENDSHIP VALUES (?, ?);"
  );
  public final SQLStmt sql2 = new SQLStmt(
	      "DELETE FROM PENDINGfriendship WHERE (inviterid=? AND inviteeid=?);"
		  );

  public VoltTable[] run( int inviterID , int inviteeID)
      throws VoltAbortException {
          voltQueueSQL( sql1, inviterID, inviteeID);
          voltQueueSQL( sql1, inviteeID, inviterID);
          voltQueueSQL( sql2, inviterID, inviteeID);
          voltExecuteSQL();
          return null;
      }
}
