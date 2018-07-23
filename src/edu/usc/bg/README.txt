README

1. Place all the files except Client.java in BG/src/edu/usc/bg. Keep the Client.java file in BG/src/edu/usc/bg/base 
Server.java is our dummy server and it can be used if Open Simulation is to be run without BG.In that case, Worker.java would need a minor change mentioned in the comments

2. All the command line arguments remain the same for create schema and load data (as used for assignment 1).

3. Only with -t option (i.e. executing simulation), add the following on command line.
 Ex: for lambda= 50req/sec, max execution time(i.e. Simulation time) as 180 sec, simulation type as open, distribution type as uniform and simulation warmup time as 0 sec
-p lambda=50 -p maxexecutiontime=180 -p simType=open -p distriType=1 -p SimWarmupTime=0

simType=closed by default and so if not specified, BG with Closed Simulation Model will be executed and all other open simulation related properties will be ignored.

4. If simType=open:

A] lambda is the rate at which requests arrive and it is 10 req/sec by default.
For uniform distribution, distriType=1 and this is the default
For random distribution, distriType=2.
For poisson distribution, distriType=3.

B] With respect to Open Simulation Model, maxexecutiontime(in seconds) defines the simulation time for which we want the requests to arrive. So we compute the number of requests that would arrive in this time given the lambda and then let the respective Distribution generate those many requests. This may run longer than the maxexecutiontime specified but it is ensured that only those many requests are generated. By default, maxexecutiontime is 0 sec.

C] SimWarmupTime is the time(in seconds) for which we want to run the Open Simulation initially before gathering any statistics. This feature is added to stabilize the system as worker threads generation overhead is reduced. It is 0 by default. 
The total simulation time=maxexecution time + SimWarmupTime

D] Statistics gathered:
Total Response Time (in milliseconds) = Summation of the response times of all requests.
Number of Requests (warmup requests are not included)
Average Response Time (in milliseconds) = (Total Response Time)/(Number of Requests)

These statistics are stored in a file for e.g. in BG/Stats-10.0.txt (in case of lambda=10)





