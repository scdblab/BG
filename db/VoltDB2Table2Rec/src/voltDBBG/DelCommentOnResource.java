package voltDBBG;


import org.voltdb.*;

public class DelCommentOnResource extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "DELETE FROM manipulation WHERE mid = ? AND rid = ?;"
  );
  
  public VoltTable[] run( int manipulationID , int resourceID)
      throws VoltAbortException {
          voltQueueSQL( sql1, manipulationID, resourceID);
          voltExecuteSQL();
          return null;
      }
}
