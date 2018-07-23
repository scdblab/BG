package edu.usc.bg.base;

import java.util.HashMap;
import java.util.Map;

public class ClientDataStats {
	private static final Double ifNull = null;
	private Double OpsTillFirstDeath = 0.0;
	private Double ActsTillFirstDeath = 0.0;
	private Double TimeTillFirstDeath = 0.0;
	private Double DumpAndValidateTime = 0.0;
	private Double NumReadOps = 0.0;
	private Double NumStaleOps = 0.0;
	private Double NumReadSessions = 0.0;
	private Double NumStaleSessions = 0.0;
	private Double NumPrunedOps = 0.0;
	private Double NumWriteOps = 0.0;
	private Double NumProcessedOps =0.0;
	private Double ValidationTime = 0.0;
	private Double DumpTime = 0.0;
	private Double FreshnessConfidence;
	
	public Double getNumWriteOps() {
		if(NumWriteOps == null) {
			return ifNull;
		}
		return NumWriteOps;
	}
	public void setNumWriteOps(Double numWriteOps) {
		NumWriteOps = numWriteOps;
	}
	
	public Double getNumProcessedOps() {
		if(NumProcessedOps == null) {
			return ifNull;
		}
		return NumProcessedOps;
	}
	public void setNumProcessesOps(Double numProcessesOps) {
		NumProcessedOps = numProcessesOps;
	}

	public Double getOpsTillFirstDeath() {
		if(OpsTillFirstDeath == null) {
			return ifNull;
		}
		return OpsTillFirstDeath;
	}
	public void setOpsTillFirstDeath(Double opsTillFirstDeath) {
		OpsTillFirstDeath = opsTillFirstDeath;
	}
	
	public Double getActsTillFirstDeath() {
		if(null == ActsTillFirstDeath)
			return ifNull;
		return ActsTillFirstDeath;
		
	}
	public void setActsTillFirstDeath(Double actsTillFirstDeath) {
		ActsTillFirstDeath = actsTillFirstDeath;
	}
	
	public Double getTimeTillFirstDeath() {
		if(TimeTillFirstDeath == null) {
			return ifNull;
		}
		return TimeTillFirstDeath;
	}
	public void setTimeTillFirstDeath(Double timeTillFirstDeath) {
		TimeTillFirstDeath = timeTillFirstDeath;
	}
	public Double getDumpAndValidateTime() {
		if(DumpAndValidateTime == null) {
			return ifNull;
		}
		return DumpAndValidateTime;
	}
	public void setDumpAndValidateTime(Double dumpAndValidateTime) {
		DumpAndValidateTime = dumpAndValidateTime;
	}
	public Double getNumReadOps() {
		if(NumReadOps == null) {
			return ifNull;
		}
		return NumReadOps;
	}
	public void setNumReadOps(Double numReadOps) {
		NumReadOps = numReadOps;
	}
	public Double getNumStaleOps() {
		if(NumStaleOps == null) {
			return ifNull;
		}
		return NumStaleOps;
	}
	public void setNumStaleOps(Double numStaleOps) {
		NumStaleOps = numStaleOps;
	}
	public Double getNumReadSessions() {
		if(NumReadSessions == null) {
			return ifNull;
		}
		return NumReadSessions;
	}
	public void setNumReadSessions(Double numReadSessions) {
		NumReadSessions = numReadSessions;
	}
	public Double getNumStaleSessions() {
		if(NumStaleSessions == null) {
			return ifNull;
		}
		return NumStaleSessions;
	}
	public void setNumStaleSessions(Double numStaleSessios) {
		NumStaleSessions = numStaleSessios;
	}
	public Double getNumPrunedOps() {
		if(NumPrunedOps == null) {
			return ifNull;
		}
		return NumPrunedOps;
	}
	public void setNumPrunedOps(Double numPrunedOps) {
		NumPrunedOps = numPrunedOps;
	}
	public Double getValidationTime() {
		if(ValidationTime == null) {
			return ifNull;
		}
		return ValidationTime;
	}
	public void setValidationTime(Double validationTime) {
		ValidationTime = validationTime;
	}
	public Double getDumpTime() {
		if(DumpTime == null) {
			return ifNull;
		}
		return DumpTime;
	}
	public void setDumpTime(Double dumpTime) {
		DumpTime = dumpTime;
	}
	public Double getFreshnessConfidence() {
		if(FreshnessConfidence == null) {
			return ifNull;
		}
		return FreshnessConfidence;
	}
	public void setFreshnessConfidence(Double freshnessConfidence) {
		FreshnessConfidence = freshnessConfidence;
	}
	
	public Map<String,Double> getStats() {
		Map<String,Double> benchmarkStats = new HashMap<String,Double>();
		benchmarkStats.put(Client.TimeTillFirstDeath, getTimeTillFirstDeath());
		benchmarkStats.put(Client.OpsTillFirstDeath, getOpsTillFirstDeath());
		benchmarkStats.put(Client.NumReadOps, getNumReadOps());
		benchmarkStats.put(Client.NumPrunedOps, getNumPrunedOps());
		benchmarkStats.put(Client.NumProcessedOps, getNumProcessedOps());//Mostly null
		benchmarkStats.put(Client.NumWriteOps, getNumWriteOps());		//Mostly null
		benchmarkStats.put(Client.ValidationTime, getValidationTime());
		benchmarkStats.put(Client.NumStaleOps, getNumStaleOps());
		benchmarkStats.put(Client.FreshnessConfidence, getFreshnessConfidence());
		
		return benchmarkStats;
	}
}