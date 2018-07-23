package voltDBBG;


import org.voltdb.*;

public class GetCreatedResources extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT * FROM resources WHERE creatorid =?;"
  );

  public VoltTable[] run( int resourceCreatorID)
      throws VoltAbortException {
          voltQueueSQL( sql1, resourceCreatorID);
          return voltExecuteSQL();
      }
}
