package edu.usc.bg.base;

import java.util.HashMap;
import java.util.Map;

public class ClientDataThreadStats {
	private static final Double ifNull = null;
	private Double OpsTillFirstDeath;
	private Double ActsTillFirstDeath;
	private Double TimeTillFirstDeath;
	private Double DumpAndValidateTime;
	private Double NumReadOps;
	private Double NumStaleOps;
	private Double NumReadSessions;
	private Double NumStaleSessions;
	private Double NumPrunedOps;
	private Double NumWriteOps;
	private Double NumProcessesOps;
	private Double ValidationTime;
	private Double DumpTime;
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
	
	public Double getNumProcessesOps() {
		if(NumProcessesOps == null) {
			return ifNull;
		}
		return NumProcessesOps;
	}
	public void setNumProcessesOps(Double numProcessesOps) {
		NumProcessesOps = numProcessesOps;
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
		benchmarkStats.put(Client.NumProcessedOps, getNumProcessesOps());//Mostly null
		benchmarkStats.put(Client.NumWriteOps, getNumWriteOps());		//Mostly null
		benchmarkStats.put(Client.ValidationTime, getValidationTime());
		benchmarkStats.put(Client.NumStaleOps, getNumStaleOps());
		benchmarkStats.put(Client.FreshnessConfidence, getFreshnessConfidence());
		
		return benchmarkStats;
	}
}