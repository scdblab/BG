package voltDBBG;


import org.voltdb.*;

public class ThawFriendship extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "DELETE FROM CONFIRMEDfriendship WHERE (friend1 = ? and friend2 = ?) OR (friend1 = ? and friend2 = ?);"
  );
  
  public VoltTable[] run( int friendid1 , int friendid2)
      throws VoltAbortException {
          voltQueueSQL( sql1, friendid1, friendid2,friendid2,friendid1);
          voltExecuteSQL();
          return null;
      }
}
