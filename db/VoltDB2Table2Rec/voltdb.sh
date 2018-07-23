#! /bin/sh

#Compile java files
echo "-------------------------------------------Compiling stored procedures-------------------------------------------";
javac ./voltdbbg/AcceptFriendship.java;
javac ./voltdbbg/CreateFriendship.java;
javac ./voltdbbg/ListFriends.java;
javac ./voltdbbg/CountFriends.java;
javac ./voltdbbg/CountPendingFriends.java;
javac ./voltdbbg/CountResources.java;
javac ./voltdbbg/GetUserDetails.java;
javac ./voltdbbg/GetFriendRequests.java;
javac ./voltdbbg/RejectFriend.java;
javac ./voltdbbg/InviteFriend.java;
javac ./voltdbbg/GetTopKResources.java;
javac ./voltdbbg/GetCreatedResources.java;
javac ./voltdbbg/GetCommentOnResource.java;
javac ./voltdbbg/PostCommentOnResource.java;
javac ./voltdbbg/DelCommentOnResource.java;
javac ./voltdbbg/ThawFriendship.java;
javac ./voltdbbg/GetPendingFriends.java;
javac ./voltdbbg/GetConfirmedFriends.java;

echo "-------------------------------------------------Compiling voltdb.sql-------------------------------------------------";
#compile voltdb.sql to create voltdb.jar
voltdb compile --classpath "./" -o voltdb.jar voltdb.sql

#run the server
voltdb create catalog voltdb.jar host localhost license /opt/voltdb/voltdb/license.xml
