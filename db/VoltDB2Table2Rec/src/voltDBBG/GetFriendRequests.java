package voltDBBG;


import org.voltdb.*;

public class GetFriendRequests extends VoltProcedure {

  public final SQLStmt sql1 = new SQLStmt(
      "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users, PENDINGfriendship WHERE inviteeid = ? AND inviterid = userid;"
  );

  public VoltTable[] run( int profileOwnerID)
      throws VoltAbortException {
          voltQueueSQL( sql1, profileOwnerID);
          return voltExecuteSQL();
      }
}
