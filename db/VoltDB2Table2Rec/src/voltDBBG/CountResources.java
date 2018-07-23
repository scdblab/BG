package voltDBBG;


import org.voltdb.*;

public class CountResources extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT count(*) FROM resources WHERE wallUserID = ?;"
  );

  public VoltTable[] run( int profileOwnerID)
      throws VoltAbortException {
          voltQueueSQL( sql1, profileOwnerID);
          return voltExecuteSQL();
      }
}
