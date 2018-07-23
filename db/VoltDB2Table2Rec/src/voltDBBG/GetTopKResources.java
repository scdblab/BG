package voltDBBG;


import org.voltdb.*;

public class GetTopKResources extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT TOP ? * FROM resources WHERE walluserid = ? ORDER BY rid DESC;"
  );

  public VoltTable[] run( int k,int profileOwnerID)
      throws VoltAbortException {
          voltQueueSQL( sql1, k,profileOwnerID);
          return voltExecuteSQL();
      }
}
