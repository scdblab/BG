package voltDBBG;


import org.voltdb.*;

public class PostCommentOnResource extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "INSERT INTO manipulation (mid, creatorid, rid, modifierid, time, type, content) VALUES (?, ?, ?, ?, ?, ?, ?);"
  );
  

  public VoltTable[] run( int mid, int creatorid, int rid, int modifierid, String timestamp, String type, String content)
      throws VoltAbortException {
          voltQueueSQL( sql1, mid, creatorid, rid, modifierid, timestamp, type, content);
          voltExecuteSQL();
          return null;
      }
}
