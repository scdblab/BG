package voltDBBG;



import org.voltdb.*;

public class CountFriends extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT count(*) FROM CONFIRMEDfriendship WHERE (friend1 = ?);"
  );

  public VoltTable[] run( int profileOwnerID)
      throws VoltAbortException {
          voltQueueSQL( sql1, profileOwnerID);
          return voltExecuteSQL();
      }
}
