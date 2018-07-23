package voltDBBG;


import org.voltdb.*;

public class ListFriends extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT userid, friend1, friend2, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users, CONFIRMEDfriendship WHERE (friend1 = ? AND userid = friend2);"
  );

  public VoltTable[] run( int profileOwnerID)
      throws VoltAbortException {
          voltQueueSQL( sql1, profileOwnerID);
          return voltExecuteSQL();
      }
}
