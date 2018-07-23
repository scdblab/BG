/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */


package edu.usc.bg.generator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Vector;

import edu.usc.bg.Member;

class Element{
	int uid = 0;
	int frequency = 0;
	double prob = 0;
	Element(int userid){
		uid = userid;
	}
	void setFreq(int freq){
		frequency = freq;
	}
	void setOrigProb(double probability){
		prob = probability;
	}

	double getProb(){
		return prob;
	}

}

class FreqComparator implements Comparator<Element> {
	public int compare(Element o1, Element o2) {
		if(o1.frequency>o2.frequency)
			return 1;
		return 0;
	}
}

class ProbComparator implements Comparator<Element> {
	public int compare(Element o1, Element o2) {
		if(o1.prob<o2.prob)
			return 1;
		return 0;
	}
}


class ClusterMember{

	int _userid;
	double _origProb;
	ClusterMember(int userid, double probability){
		_userid = userid;
		_origProb = probability;
	}

	public int getUserid(){
		return _userid;
	}


	public double getOrigProb(){
		return _origProb;
	}

}

class Cluster{
	Vector<ClusterMember> members = new Vector<ClusterMember>();
	double totalProb ;
	boolean completed = false;

	public Cluster(){
		totalProb = 0;
	}

	public void addMember(ClusterMember m){
		members.add(m);
	}

	public void addProb(double prob){
		totalProb = totalProb + prob;
	}


	public double getTotalProb(){
		return totalProb;
	}

	public void setCompleted(){
		completed = true;
	}
	public boolean isCompleted(){
		return completed;
	}

}

class ClusterComparator implements Comparator<Cluster>{

	@Override
	public int compare(Cluster arg0, Cluster arg1) {
		if(arg0.totalProb>arg1.totalProb)
			return 1;
		return 0;
	}

}


/**
 * Generates the clusters for the various BG client so the clients will not overlap
 * These clusters are generated using the dzipfian approach
 * @author barahman
 *
 */

public class Fragmentation {
	private Double[] origProbs ;
	private Double[] normalizedProbs ;
	private double PercentageOfMembers = 0.2;
	private int interestedUserCount = 0;
	private Double[] clusterProbs;	
	Cluster[] clusters;
	DistOfAccess myDist;
	int[] myMemberIds;
	int _machineid = 0;
	int _numShards = 0;
	boolean verbose=true;

	public Cluster[] getClusters(){
		return clusters;
	}

	public int[] getMyMembers(){
		return myMemberIds;
	}

	public DistOfAccess getMyDist(){
		return myDist;
	}

	public int getMemberIdForIdx(int idx){
		return clusters[_machineid].members.get(idx).getUserid();
	}

	public Fragmentation(int usercount, int numBGClients, int machineid, String probs, double ZipfianMean, boolean verbo){
		this.verbose=verbo;
		_machineid = machineid;
		interestedUserCount = (int)(PercentageOfMembers * usercount);
		origProbs = new Double[usercount];
		normalizedProbs = new Double[usercount];
		clusterProbs = new Double[usercount];
		//number of clusters is equal to number of clients
		int numClusters = numBGClients;
		interestedUserCount = (int)(PercentageOfMembers * usercount);
		
		Double[] props;
		double weight, idealProbSum;
		Object[] quickHack = new Object[2];
		quickHack = initPropsAndWeight(numBGClients, probs);
		props = (Double[]) quickHack[0];
		weight = new Double(quickHack[1].toString());
		idealProbSum= 1.0/weight;

		//Initialize the global var clusters
		initClusters(numClusters);
		
		//create initial zipfian distribution
		DistOfAccess dist = createZipfianDistribution(usercount, ZipfianMean);

		//trying to create clusters with weighted total probability
		createClusters(usercount, numClusters, props, idealProbSum, dist);
		
		//create the normalized probs with one BGClient
		createNormalizedProbability(numClusters, props, weight);

		//printing the elements in each cluster for testing
		if (verbose){
		for(int i =0; i< numClusters; i++){
			System.out.println("ClusterId"+i+", "+clusters[i].members.size());
		}
		}
		//populate my list of members
		initMemberIds(machineid);

		//compute the initial probabilities of the highest 20% of users
		//it would already be sorted
		double origtmpSum = initTop20Probability();
		if (verbose)
		System.out.println("ZipfianMean="+ZipfianMean+" Usercount="+usercount+" numMaxClusters="+numClusters +"; "+(100.0*PercentageOfMembers)+"% of members have, "+((double)(origtmpSum))+", initial probability.");

		//compute the normalized probabilities of the highest 20% of users
		double newNormTmpSum = computeTop20NormalizedProbability();
		if (verbose)
		System.out.println("ZipfianMean="+ZipfianMean+" Usercount="+usercount+" numMaxClusters="+numClusters +"; "+(100.0*PercentageOfMembers)+"% of members have, "+((double)(newNormTmpSum))+" clustering(new)probability.");

		Double[] newProbs = new Double[clusters[machineid].members.size()];
		for(int t=0; t<clusters[machineid].members.size(); t++){
			newProbs[t] = clusterProbs[clusters[machineid].members.get(t).getUserid()];
		}
		
		myDist = dist;
		myDist.reWriteProbs(clusters[machineid]);
	}

	/**
	 * return
	 */
	private double computeTop20NormalizedProbability() {
		Element[] sortedNormElements = null;
		sortedNormElements = new Element[normalizedProbs.length];
		for(int i=0; i<normalizedProbs.length; i++){
			Element elm = new Element(i);
			elm.setOrigProb(normalizedProbs[i]);
			sortedNormElements[i] = elm;
		}
		Arrays.sort(sortedNormElements, new ProbComparator());
		double newNormTmpSum = 0;
		for(int i=0; i<interestedUserCount;i++){	
			newNormTmpSum+=sortedNormElements[i].getProb();
		}
		return newNormTmpSum;
	}

	/**
	 * return
	 */
	private double initTop20Probability() {
		double origtmpSum = 0;
		for(int i=0; i<interestedUserCount;i++){	
			origtmpSum+=origProbs[i];
		}
		return origtmpSum;
	}

	/**
	 *populate my list of members
	 * @param machineid
	 */
	private void initMemberIds(int machineid) {
		myMemberIds = new int[clusters[machineid].members.size()];
		
		//populate my list of members
		for(int i=0; i<clusters[machineid].members.size(); i++){
			myMemberIds[i]=(clusters[machineid].members.get(i).getUserid());
		}
	}

	/**
	 * //create the normalized probs with one BGClient
	 * @param numClusters
	 * @param props
	 * @param weight
	 */
	private void createNormalizedProbability(int numClusters, Double[] props,
			double weight) {
		for(int i =0; i< numClusters; i++){
			for(int j=0; j<clusters[i].members.size();j++){
				//create the normalized probs with one BGClient
				int memberUserId = clusters[i].members.get(j).getUserid();
				double memberOrigProbability = clusters[i].members.get(j).getOrigProb();
				double clusterTotalProbability = clusters[i].getTotalProb();
				double memberClusterProb = memberOrigProbability/clusterTotalProbability;
				normalizedProbs[memberUserId] = memberClusterProb * (props[i]/weight);
				clusterProbs[memberUserId] = memberClusterProb;
			}
		}
	}

	/**
	 * Creating clusters with weighted probabilities
	 * The lowest userid has the highest prob
	 * @param usercount
	 * @param numClusters
	 * @param props
	 * @param idealProbSum
	 * @param dist
	 */
	private void createClusters(int usercount, int numClusters, Double[] props,
			double idealProbSum, DistOfAccess dist) {
		//the lowest userid has the highest prob
		int head = 0;
		int tail = usercount-1;
		int clusterid = numClusters-1;
		int numCompleted = 0;
		while(head <= tail){
			ClusterMember m = new ClusterMember(head,dist.GetProbability(head+1) );
			double r = props[clusterid];
			if((clusters[clusterid].totalProb + m.getOrigProb()) > (idealProbSum * r) ){
				m = new ClusterMember(tail,dist.GetProbability(tail+1) );
				if((clusters[clusterid].totalProb + m.getOrigProb()) > (idealProbSum * r) ){
					if(clusterid != numClusters-1){
						clusters[clusterid].setCompleted();
						numCompleted++;
					}
					if(numCompleted == (numClusters-1)){
						clusterid = numClusters-1;
						clusters[clusterid].addMember(m);
						clusters[clusterid].addProb(m.getOrigProb());	
						tail--;
					}
				}else{
					clusters[clusterid].addMember(m);
					clusters[clusterid].addProb(m.getOrigProb());	
					tail--;
				}
			}
			else{
				clusters[clusterid].addMember(m);
				clusters[clusterid].addProb(m.getOrigProb());
				head++;
			}

			if(numCompleted != (numClusters-1)){
				clusterid--;
				clusterid = (clusterid+numClusters)%numClusters;
				while(clusters[clusterid].isCompleted()){
					clusterid--;
					clusterid = (clusterid+numClusters)%numClusters;
				}
			}else{
				clusterid = numClusters-1;
			}
		}
		if (verbose)
		System.out.println("Done creating clusters.");
	}

	/**
	 * @param numClusters
	 */
	private void initClusters(int numClusters) {
		//initiating the array keeping track of the clusters
		clusters = new Cluster[numClusters];
		for(int i=0; i< numClusters; i++){
			clusters[i] = new Cluster();;
		}
	}

	/**
	 * @param usercount
	 * @param ZipfianMean 
	 * @param dist
	 * return 
	 */
	private DistOfAccess createZipfianDistribution(int usercount, double ZipfianMean) {
		if (verbose)
		System.out.println("Creating the initial zipfian probabilities..." );
		double sumOrigProb = 0;
		DistOfAccess dist = new DistOfAccess(usercount,"Zipfian",true,ZipfianMean);

		for(int i=0; i<usercount; i++){
			origProbs[i] = dist.GetProbability(i+1);
			sumOrigProb += dist.GetProbability(i+1);
		}
		if ( verbose){
		System.out.println("Sum of original probs = "+sumOrigProb );
		System.out.println("Initial Zipfian probabilities are created." );
		}
		return dist;
	}

	/**
	 * @param numBGClients
	 * @param probs
	 * @param props
	 */
	private Object[] initPropsAndWeight(int numBGClients, String probs) {
		Double[] props = new Double[numBGClients];
		double weight = 0;
		if(probs != ""){
			String strprops[] = probs.split("@");
			for(int i=0; i<strprops.length; i++){
				props[i] = Double.parseDouble(strprops[i]);
				weight += props[i];
			}
		}else{
			for(int i=0; i<numBGClients; i++) {
				props[i]=1.0;
				weight += props[i];
			}
		}
		Object[] quickHack = new Object[2];
		quickHack[0] = props;
		quickHack[1] = weight;
		return quickHack;
	}


	public static void main(String[] args){
		int usercount = 10000;
		int numBGClients = 16;
		String probs = "";
		double ZipfianMean = 0.27;
		DistOfAccess myDist;
		int numReqs = 100000000;
		int[] frequencies = new int[usercount];

		//construct the probability string
		//assuming all have the same prob
		for(int i=0; i<numBGClients; i++){
			probs+=(1.0/numBGClients)+"@";
		}
		//create the fragments for each BG client
		//for every client create its distribution and issue its requests
		for(int i=0; i<numBGClients; i++){
			Fragmentation createFrags = new Fragmentation(usercount, numBGClients,i,probs, ZipfianMean,true);
			myDist = createFrags.getMyDist();
			int[] myMembers = createFrags.getMyMembers();
			Member[] myMemberObjs = new Member[myMembers.length];
			for(int j=0; j<myMembers.length;j++){
				//assuming there is one shard per client (as we are trying to remove the effect of activation and deactivation)
				Member newMember = new Member(myMembers[j], j, 0 , j);
				myMemberObjs[j] = newMember;
			}
			//issue requests for this client
			for(int j=0; j<numReqs; j++){
				int idx = myDist.GenerateOneItem()-1;
				int key = myMembers[idx];
				frequencies[key]++;
			}
		}
		try {
			String fileName = "DZipfian"+usercount+"-"+numBGClients+"-"+(numBGClients*numReqs)+".txt";
			FileWriter fstream = new FileWriter(fileName);
			BufferedWriter out = new BufferedWriter(fstream);
			//print the frequencies
			for(int i=0; i<usercount; i++){
				System.out.println((i+1)+", "+frequencies[i]);
				out.write((i+1)+", "+frequencies[i]+"\n");
			}
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
