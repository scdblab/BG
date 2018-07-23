package voltDBBG;


import org.voltdb.*;

public class GetCommentOnResource extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT * FROM manipulation WHERE rid = ?;"
  );

  public VoltTable[] run( int resourceID)
      throws VoltAbortException {
          voltQueueSQL( sql1, resourceID);
          return voltExecuteSQL();
      }
}
