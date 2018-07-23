package voltDBBG;


import org.voltdb.*;

public class CreateFriendship extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "INSERT INTO CONFIRMEDFRIENDSHIP VALUES (?, ?);"
  );
  

  public VoltTable[] run( int memberA , int memberB)
      throws VoltAbortException {
          voltQueueSQL( sql1, memberA, memberB);
          voltQueueSQL( sql1, memberB, memberA);
          
          voltExecuteSQL();
          return null;
      }
}
