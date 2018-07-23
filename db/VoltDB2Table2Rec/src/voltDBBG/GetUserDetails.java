package voltDBBG;


import org.voltdb.*;

public class GetUserDetails extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users WHERE UserID = ?;"
  );

  public VoltTable[] run( int profileOwnerID)
      throws VoltAbortException {
          voltQueueSQL( sql1, profileOwnerID);
          return voltExecuteSQL();
      }
}
