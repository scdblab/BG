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


package edu.usc.bg;
/**
 * Keeps a track of the members in the social network, their unique memberid,
 *  the shard they belong to and their index in the shard
 * @author barahman
 *
 */
public class Member{
	int _uid;
	int _actualIdxInMyMembers;
	int _shardIdx;
	int _idxInShard;
	
	public int get_uid() {
		return _uid;
	}

	
	public int get_actualIdxInMyMembers() {
		return _actualIdxInMyMembers;
	}

	public int get_shardIdx() {
		return _shardIdx;
	}
	public int get_idxInShard() {
		return _idxInShard;
	}
	public Member(int uid, int idx, int shardIdx, int idxInShard){
		_uid = uid;
		_actualIdxInMyMembers = idx;
		_shardIdx = shardIdx;
		_idxInShard=idxInShard;
	}
	
		
}