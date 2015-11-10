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
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{

	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	private synchronized TimestampVector getTimestampVector(String node){
		
		return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public synchronized void updateMax(TimestampMatrix tsMatrix){
		
		Iterator it = tsMatrix.timestampMatrix.keySet().iterator();
		
		while (it.hasNext()) {
			String key = (String) it.next();
			TimestampVector other = tsMatrix.getTimestampVector(key);
			
			TimestampVector value = this.timestampMatrix.get(key);
			
			if (value != null) {
				value.updateMax(other);
			}
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public synchronized void update(String node, TimestampVector tsVector){
		
		this.timestampMatrix.replace(node, tsVector);
	}
	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public synchronized TimestampVector minTimestampVector(){
		
		TimestampVector tsVector = null;
		
		for (TimestampVector ackVector : this.timestampMatrix.values()) {
			
			if (tsVector == null) {
				
				tsVector = ackVector.clone();
			} 
			
			else {
				
				tsVector.mergeMin(ackVector);
			}
		}
		
		return tsVector;
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampMatrix clone(){
		
		Object[] hosts = (this.timestampMatrix.keySet()).toArray();
		String[] participants = Arrays.copyOf(hosts,hosts.length, String[].class);
		List<String> listParticipants = Arrays.asList(participants);
		
		TimestampMatrix clone = new TimestampMatrix(listParticipants);
		
		for (String hostid: this.timestampMatrix.keySet()) {
			clone.update(hostid, this.timestampMatrix.get(hostid));
		}
		
		return clone;
	}
	
	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		
		if (obj == null) {
			
			return false;
		}
		
		else if (this == obj) {
			
			return true;
		}
		
		else if (!(obj instanceof TimestampMatrix)) {
			
			return false;
		}
		
		TimestampMatrix newAck = (TimestampMatrix) obj;
		
		if (this.timestampMatrix == newAck.timestampMatrix) {
			
			return true;
		} 
		
		else if (this.timestampMatrix == null || newAck.timestampMatrix == null) {
			
			return false;
		} 
		
		else {
			
			return this.timestampMatrix.equals(newAck.timestampMatrix);
		}

	}

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
