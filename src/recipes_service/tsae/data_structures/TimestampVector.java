/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{

	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}

	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public synchronized void updateTimestamp(Timestamp timestamp){
		
		if (timestamp !=null) {
			
			timestampVector.replace(timestamp.getHostid(), timestamp);
			
		}
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector){
		
		if (tsVector == null) {
			
			return;
					
		}
		
		for (String hostid: timestampVector.keySet()) {
			
			Timestamp other = tsVector.getLast(hostid);
			
			if (other == null){
				
				continue;
			} 
			
			else if (other.compare(this.getLast(hostid)) > 0) {
				
				this.timestampVector.replace(hostid, other);
			}
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public synchronized Timestamp getLast(String node){
		
		return timestampVector.get(node);
	}
	
	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public synchronized void mergeMin(TimestampVector tsVector){
		
		if (tsVector == null ) {
			
			return;
		}
		
		for (String hostid: timestampVector.keySet()) {
			
			Timestamp other = tsVector.getLast(hostid);
			
			if (other == null){
			
				this.timestampVector.put(hostid, other);
			} 
			
			else if (other.compare(this.getLast(hostid)) < 0) {
				
				this.timestampVector.replace(hostid, other);
			}
		}
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampVector clone(){
		
		
		Object[] hosts = (this.timestampVector.keySet()).toArray();
		String[] participants = Arrays.copyOf(hosts,hosts.length, String[].class);
		List<String> listParticipants = Arrays.asList(participants);
		
		TimestampVector newTsVector = new TimestampVector(listParticipants);
		
		for (String hostid: this.timestampVector.keySet()) {
			
			newTsVector.updateTimestamp(this.getLast(hostid));
		}
		
		return newTsVector;				
	}
	
	/**;
	 * 
	 * equals
	 */
	
	public synchronized boolean equals(TimestampVector tsVector){
		
		if (this.timestampVector == tsVector.timestampVector) {
			
			return true;
		}
		
		else if (this.timestampVector == null || tsVector.timestampVector == null) {
			
			return false;
		} 
		
		else {
			
			return (this.timestampVector).equals(tsVector.timestampVector);
		}
	}
	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		
		String all="";
		if(timestampVector==null){
			
			return all;
		}
		
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
			
				all+=timestampVector.get(name)+"\n";
		}
		
		return all;
	}
}
