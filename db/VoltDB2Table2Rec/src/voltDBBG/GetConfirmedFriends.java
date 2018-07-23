package voltDBBG;


import org.voltdb.*;

public class GetConfirmedFriends extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT FRIEND2 from CONFIRMEDfriendship where (FRIEND1=?);"
  );

  public VoltTable[] run( int profileId)
      throws VoltAbortException {
          voltQueueSQL( sql1, profileId);
          return voltExecuteSQL();
      }
}
