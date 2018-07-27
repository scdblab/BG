# BG
BG is a benchmark to evaluate performance of a data store for interactive social networking actions and sessions.  These actions and sessions either read or update a very small amount of the entire data set.

### Authors  
Yazeed Alabdulkarim,  Sumita Barahmand, and Shahram Ghandeharizadeh

### Description

The documentation for this version of BG is similar to the one posted at [bgbenchmark.org](http://bgbenchmark.org/) with the following extensions:

1) Three decentralized techniques for generating meaningful actions.

2) Disjoint Database scales superlinearly as a function of nodes.

3) Integrated Database (Retain and Delegate) maybe more suitable to evaluate certain classes of data stores, see introduction of [database lab technical report  2018-01](http://dblab.usc.edu/Users/papers/bgscalable.pdf).

4) A hybrid technique that combines the strengths of Disjoint and Integrated Database.

For a complete description, see [database lab technical report  2018-01](http://dblab.usc.edu/Users/papers/bgscalable.pdf).

Running this version of BG is similar to the one posted at [bgbenchmark.org](http://bgbenchmark.org/) with the following additional parameters:

<table>
  <tr>
    <td><strong>Parameter</strong></td>
    <td><strong>Value</strong></td>
    <td><strong>Description</strong></td>
  </tr>
  <tr>
    <td>benchmarkingmode</td>
    <td>disjoint, retain, delegate, hybridretain, or hybriddelegate</td>
    <td>Specifies the technique for generating actions. The default is disjoint</td>
  </tr>
  <tr>
    <td>clients</td>
    <td>IP1:Port1, IP2:Port2,....</td>
    <td>List of IP and port pairs for BG clients</td>
  </tr>
  <tr>
    <td>numsockets</td>
    <td>An integer greater than 0</td>
    <td>The initial number of TCP sockets to created with every other BG client with  Integrated Database and hybrid techniques. The default value is 10</td>
  </tr>
</table>


For completeness, we provide a description of all BG parameters below in two tables.  The first table lists essential parameters that must be specified when running BG.  The second table lists the optional parameters.

Here are the essential parameters that must be specified when running this version of BG: 

<table>
  <tr>
    <td><strong>Parameter</strong></td>
    <td><strong>Value</strong></td>
    <td><strong>Description</strong></td>
  </tr>
  <tr>
    <td>-db</td>
    <td>The name of the data store client class</td>
    <td>Identifies the data store interface layer which is going to be benchmarked</td>
  </tr>
  <tr>
    <td>threadcount	</td>
    <td>An integer greater than 0	</td>
    <td>The simultaneous socialites emulating members issuing requests</td>
  </tr>
  <tr>
    <td>maxexecutiontime</td>
    <td>An integer greater than 0</td>
    <td>Identifies the maximum benchmarking execution time in seconds</td>
  </tr>
  <tr>
    <td>numclients</td>
    <td>An integer greater than 0	</td>
    <td>The number of BG Clients in the system. If not specified numclients=1 will be assumed</td>
  </tr>
  <tr>
    <td>machineid</td>
    <td>An integer greater than or equal to 0	</td>
    <td>The machineid is the BG Client identifier and should start from 0. It represents the index of this client in the clients’  IP and port list</td>
  </tr>
  <tr>
    <td>confperc</td>
    <td>A value between 0 to 1</td>
    <td>The percentage of confirmed friendships among all the friendship relationships created for members</td>
  </tr>
  <tr>
    <td>initapproach</td>
    <td>querydata/deterministic</td>
    <td>"querydata" instructs BG to query the initial state of the data store before running benchmark. 
"deterministic" instructs BG to construct the intial state of the data store using information about the load phase. This parameter should be accompanied with resourcecountperuser, friendcountperuser, confper, numloadthreads and useroffset.</td>
  </tr>
  <tr>
    <td>friendcountperuser</td>
    <td>An even integer number greater than or equal to zero</td>
    <td>The number of friendship relationships per member (This can be confirmed friendships, generated friendships or pending friendships).

</td>
  </tr>
  <tr>
    <td>resourcecountperuser</td>
    <td>An integer number greater than or equal to zero</td>
    <td>The number of resources created by each member</td>
  </tr>
  <tr>
    <td>numloadthreads </td>
    <td>An integer number greater than or equal to zero</td>
    <td>The number of threads used in the load phase per BG client. This parameter should be accompanied with the initapproach=deterministic parameter.</td>
  </tr>
  <tr>
    <td>usercount</td>
    <td>An integer greater than 0	
</td>
    <td>Number of members in the data store.
The members are assigned memberids in a sequential manner.
</td>
  </tr>
  <tr>
    <td>useroffset</td>
    <td>An integer number greater than or equal to zero</td>
    <td>The smallest memberid in the data store. Can be used when using multiple BG Clients to populate the data store. It will be considered as 0 if not specified</td>
  </tr>
  <tr>
    <td>zipfianmean</td>
    <td>A value between 0 to 1, defining the skewness of the dzipfian distribution.</td>
    <td>If the dzipfian distribution is specified the mean specified by this parameter will be used. The default value is 0.27
</td>
  </tr>
  <tr>
    <td>warmup</td>
    <td>An integer greater than or equal to zero</td>
    <td>The number of warmup operations issued to the data store. These operations are issued before the actual benchmarking phase starts and are not considered in the measurements. The default value for this is 0 operations.</td>
  </tr>
  <tr>
    <td>warmupthreads</td>
    <td>An integer greater than zero</td>
    <td>Identifies the number of threads to issue the warmup operations</td>
  </tr>
  <tr>
    <td>-P</td>
    <td>Name of the workload file	</td>
    <td>The name of the workload file containing the runtime workload parameters.</td>
  </tr>
  <tr>
    <td>requestdistribution</td>
    <td>uniform/dzipfian/latest</td>
    <td>Identifies the distribution of the members generating requests.
</td>
  </tr>
</table>

Here are the optional parameters to customize BG:

<table>
  <tr>
    <td><strong>Parameter</strong></td>
    <td><strong>Value</strong></td>
    <td><strong>Description</strong></td>
  </tr>
 <tr>
                    <td>AcceptFrdReqSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, LF, VFR, AFR, LF, VFR.</p></td>
                </tr>
                <tr>
                    <td>AcceptFriendReqAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the AFR action.</p></td>
                </tr>
                <tr>
                    <td>ViewFrdProfileSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, LF, VP.</p></td>
                </tr>
                <tr>
                    <td>ViewSelfProfileSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, VCR.</p></td>
                </tr>
                <tr>
                    <td>DeleteCmtOnResSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, VP, VTR ,VCR, DCR, VCR.</p></td>
                </tr>
                <tr>
                    <td>DeleteCommentOnResourceAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the DCR action.</p></td>
                </tr>
                <tr>
                    <td>PostCmtOnResSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, VP, VTR ,VCR, PCR, VCR.</p></td>
                </tr>
                <tr>
                    <td>expectedlatency</td>
                    <td>A value greater than or equal to 0</td>
                    <td><p>The BG client uses the defined expected latency to compute the confidence for the various operations it performs in the benchmarking phase.</p></td>
                </tr>
                <tr>
                    <td>exportfile</td>
                    <td>File name</td>
                    <td><p>The name of the output file. If the output file is not specified the output will be written to the console.</p></td>
                </tr>
                <tr>
                    <td>InviteFrdSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, LF, IF, VFR</p></td>
                </tr>
                <tr>
                    <td>ViewCommentsOnResourceAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the VCR action</p></td>
                </tr>
                <tr>
                    <td>ListFriendsAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the LF action</p></td>
                </tr>
                <tr>
                    <td>ViewFriendReqAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the VFR action</p></td>
                </tr>
                <tr>
                    <td>ViewProfileAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the VP action</p></td>
                </tr>
                <tr>
                    <td>ViewTopKResourcesAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the VTR action</p></td>
                </tr>
                <tr>
                    <td>imagesize</td>
                    <td>An integer number greater than 0</td>
                    <td><p>Identifies the image size in KB to be inserted for members in the load phase. BG will insert a random set of bytes corresponding to the image size specified for each member as an image. BG always inserts 2KB random bytes as the thumbnail image for the members.</p></td>
                </tr>
                <tr>
                    <td>insertimage</td>
                    <td>True/False</td>
                    <td><p>Identifies if images will be inserted/retrieved for members or not. These images will be used as thumbnails as well as member profile images.</p></td>
                </tr>
                <tr>
                    <td>interarrivaltime</td>
                    <td>An integer greater than or equal to zero</td>
                    <td><p>the time between the various user sessions. 0 will be considered if not specified.</p></td>
                </tr>
                <tr>
                    <td>InviteFriendAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the IF action</p></td>
                </tr>
                <tr>
                    <td>logdir</td>
                    <td>log directory path</td>
                    <td><p>Specifies the directory for the log records. In the benchmarking phase the log files will be created in this directory and will be used in the validation phase.</p></td>
                </tr>
                <tr>
                    <td>monitor</td>
                    <td>An integer value greater than or equal to 0</td>
                    <td><p>The duration at which the observed throughput is reported to the coordinator. This is used when the BG Client is in the rating mode.</p></td>
                </tr>
                <tr>
                <tr>
                    <td>operationcount</td>
                    <td>An integer greater than 0</td>
                    <td><p>Number of social operations(actions/sessions) to be performed.</p></td>
                </tr>
                <tr>
                    <td>port</td>
                    <td>A valid port</td>
                    <td><p>The port number on which the BG clients starts communicating with the BG listener.</p></td>
                </tr>
                <tr>
                    <td>PostCommentOnResourceAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the PCR action.</p></td>
                </tr>
                <tr>
                    <td>probs</td>
                    <td>A string containing the various BG client rates separated by &ldquo;@&rdquo; and accompanied by a terminating &ldquo;@&rdquo;. i.e. If there are 3 clients we will have something similar to 0.3@0.6@0.1@</td>
                    <td><p>This is used for multi-node benchmarking. It specifies the rate at which all BG Clients issue request and is used in the generation of fragments for the dzipfian distribution.</p></td>
                </tr>
                <tr>
                    <td>ratingmode</td>
                    <td>True/false</td>
                    <td><p>If the BG Client is used for rating and needs to coordinate with a coordinator this parameter needs to be set to true.</p></td>
                </tr>
                <tr>
                    <td>RejectFrdReqSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, LF, VFR, RFR, LF, VFR.</p></td>
                </tr>
                <tr>
                    <td>RejectFriendReqAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the RFR action.</p></td>
                </tr>
                <tr>
                    <td>thinktime</td>
                    <td>An integer greater than or equal to zero</td>
                    <td><p>The think time between emulated member clicks. 0 will be considered if not specified.</p></td>
                </tr>
                <tr>
                    <td>ThawFrdshipSession</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the session containing the following actions in the specified order: VP, VTR, LF, TF, LF.</p></td>
                </tr>
                <tr>
                    <td>ThawFriendshipAction</td>
                    <td>A value between 0 to 1</td>
                    <td><p>Identifies the portion of requests performing the TF action.</p></td>
                </tr>
                <tr>
                    <td>validation.driver</td>
                    <td>database driver</td>
                    <td><p>Used when validation using the RDBMS. Identifies the database driver.</p></td>
                </tr>
                <tr>
                    <td>validation.passwd</td>
                    <td>database password</td>
                    <td><p>Used when validation using the RDBMS. Identifies the database password.</p></td>
                </tr>
                <tr>
                    <td>validation.url</td>
                    <td> database url</td>
                    <td><p>Used when validation using the RDBMS. Identifies the database URL.</p></td>
                </tr>
                <tr>
                    <td>validation.user</td>
                    <td>database username</td>
                    <td><p>Used when validation using the RDBMS. Identifies the database username.</p></td>
                </tr>
                <tr>
                    <td>validationapproach</td>
                    <td>RDBMS/interval</td>
                    <td><p>Identifies the validation approach to be used. The interval option uses the interval tree approach to validate the log records.</p></td>
                </tr>
                <tr>
                    <td>validationblock</td>
                    <td>An integer greater than zero</td>
                    <td><p>The number of log records validated by each validation thread.</p></td>
                </tr>
                <tr>
                    <td>validationthreads</td>
                    <td>An integer greater than zero</td>
                    <td><p>Number of simultaneous threads used to validate log records.</p></td>
                </tr>
				<tr>
                    <td>enablelogging</td>
                    <td> true/false, enables generation of log records.</td>
                    <td><p>If the enablelogging is set to true, log files will be generated for actions which can then be used to compute the amount of stale/unpredictable data.</p></td>
                </tr>
   <tr>
                    <td>-load</td>
                    <td>None</td>
                    <td><p>Instructs BG to populate the data store.</p></td>
                </tr>
                <tr>
                    <td>-loadindex</td>
                    <td>None</td>
                    <td><p>Instructs BG to populate the data store and create the appropriate index structures.</p></td>
                </tr>
                <tr>
                    <td>-s</td>
                    <td>None</td>
                    <td><p>If specified, BG will report throughput and the observed average response time for the operations performed for 10 second windows throughout the benchmark execution.</p></td>
                </tr>
                <tr>
                    <td>-schema</td>
                    <td>None</td>
                    <td><p>Instructs BG to create the data store schema.</p></td>
                </tr>
                <tr>
                    <td>-testdb</td>
                    <td>None</td>
                    <td><p>Instructs BG to test the connection to the data store.</p></td>
                </tr>
                <tr>
                    <td>-stats</td>
                    <td>None</td>
                    <td><p>Instructs BG to query the status of the data store.</p></td>
                </tr>

</table>
