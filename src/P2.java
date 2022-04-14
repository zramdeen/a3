/*=============================================================================
| Assignment: Atmospheric Temperature Reading Module.
|
| Author: Zahid Ramdeen
| Language: Java
|
| To Compile: (from terminal)
| javac P2.java
|
| To Execute: (from terminal)
| java P2
|
|	Time Calculations
|		1hour = 1second = 1000ms
|
| Customize: (optional)
| 	modify the "N" variable in the program to set total Sensors
| 	modify the "totalExecutionTimeMillis" to set the total execution time.
|
|	Issues:
|		currently the max-10-min difference does not work.
|
| Class: COP4520 - Concepts of Parallel and Distributed Processing - Spring 2022
| Instructor: Damian Dechev
| Due Date: 4/13/2022
|
+=============================================================================*/

import java.sql.Timestamp;
import java.util.*;

public class P2 {
	public static void main(String[] args) throws Exception {
		/*
			time simulation
			1hr = 1second

			thus 1min = 16ms
			10min = 160ms
		 */


		int N = 8;
		int totalExecutionTimeMillis = 5000;
		int totalMins = totalExecutionTimeMillis / 1000;
		boolean recorded[][] = new boolean[N][totalMins];

		// create the sensors objects
		Ant[] ants = new Ant[N];
		for (int i = 0; i < N; i++) {
			ants[i] = new Ant(totalExecutionTimeMillis, 60, recorded[i]);
		}

		// the manager object
		Queen queen = new Queen(ants, recorded);

		// the threads
		Thread[] tarr = new Thread[N+1];
		tarr[N] = new Thread(queen);
		for (int i = 0; i < N; i++) {
			tarr[i] = new Thread(ants[i]);
		}

		// start the threads
		for (Thread t:tarr) {
			t.start();
		}
	}
}

/**
 * The Ant class represents a Sensor.
 * An Ant has its own wait-free queue and records its temperatures without delay to the queue.
 * After a minute of recording has passed (represents an hour), it signals the Queen to process it.
 */
class Ant implements Runnable {
	int totalExecutionTimeMillis;
	int interval;
	int capacity;
	int recordingsPerMin;
	int excessWaitTime;
	boolean[] recorded;
	WaitFreeQueue<Recording> q;

	public Ant(int totalExecutionTimeMillis, int recordingsPerMin, boolean[] recorded) {
		this.totalExecutionTimeMillis = totalExecutionTimeMillis;
		this.recordingsPerMin = recordingsPerMin;
		this.interval = 1000 / recordingsPerMin;
		this.excessWaitTime = 1000 - (interval * recordingsPerMin);
		capacity = recordingsPerMin * 5; // add 5 times the capacity just in case
		this.recorded = recorded;
		q = new WaitFreeQueue<>(capacity);
	}

	@Override
	public void run() {
		int i = 0;
		int curMin = 0;
		int totalRecordings = recordingsPerMin * (totalExecutionTimeMillis / 1000);
		int j = 0;
		while(j < totalRecordings) {
			j++;
			try {
				// get a recording
				q.enq(new Recording(Recording.randTemp(-100, 70)));
				i++; // advance recording counter
				// reset i and mark boolean arr as finished
				if(i >= recordingsPerMin){
					recorded[curMin] = true; // mark current min as finished
					i = 0;
					curMin++;
				}


				// wait before getting new recording
				Thread.sleep(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

/**
 * The Queen class represents a Manager that processes the work from the Ants.
 * The queen calculates the total number of items it must process.
 * It then waits for the Ant to signal it before it starts processing.
 * This allows for slow threads to complete their work.
 */
class Queen implements Runnable {
	Ant[] ants;
	int totalWork;
	int recordingsPerMin;
	int interval;
	LinkedList<LinkedList<Recording>> list;
	boolean[][] recorded;

	Queen(Ant[] ants, boolean[][] recorded){
		this.ants = ants;
		this.recorded = recorded;
		totalWork = recorded[0].length;
		recordingsPerMin = ants[0].recordingsPerMin;
		interval = 1000; // checks will take place each minute
		list = new LinkedList<>();
	}

	@Override
	public void run() {
		for (int i = 0; i < totalWork; i++) {
			// wait until ants complete their work
			while(!recordingsReady(i)){
				try {
					Thread.sleep(interval);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			// process the work from each ant
			list.add(i, new LinkedList<>());
			for (int j = 0; j < ants.length; j++) {
				try {
					for (int k = 0; k < recordingsPerMin; k++) {
						list.get(i).add(ants[j].q.deq());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			// sort the consolidated data
//			Collections.sort(list.get(i));
			Recording[] currEpochTimestampSorted = list2arr(list.get(i));
			Recording[] currEpochTempSorted = list2arr(list.get(i));

			Arrays.sort(currEpochTimestampSorted);
			Arrays.sort(currEpochTempSorted, new Comparator<Recording>() {
				@Override
				public int compare(Recording o1, Recording o2) {
					return Integer.compare(o1.temp, o2.temp);
				}
			});

			// the highest 5
			Recording[] highest = highest5(currEpochTempSorted);
			System.out.print("highest 5: ");
			System.out.println(Arrays.toString(highest));

			// the lowest 5
			Recording[] lowest = lowest5(currEpochTempSorted);
			System.out.print("lowest 5: ");
			System.out.println(Arrays.toString(lowest));

			// the largest temperature difference
			Recording[] largestDiff = largest10MinDifference(currEpochTimestampSorted);
			System.out.print("largest 10 min diff: ");
			System.out.println(Arrays.toString(largestDiff));
			int millisDiff = (int) Math.abs(largestDiff[0].time.getTime() - largestDiff[1].time.getTime());
			System.out.printf("ms diff: %d\tt0: %s\tt1: %s\n", millisDiff, largestDiff[0].time, largestDiff[1].time);

			System.out.println("-------------------------");
		}

		System.out.println("done");
	}

	private boolean recordingsReady(int index){
		boolean ans = true;

		for (int i = 0; i < ants.length; i++) {
			ans &= recorded[i][index];
		}

		return ans;
	}

	private Recording[] largest10MinDifference(Recording[] sortedByTime){
		int lo = 0;
		int hi = sortedByTime.length-1; // leave at least one element in the arr

		int diff = Integer.MIN_VALUE;
		Recording[] res = highestDiffFromIndex(sortedByTime, 0);
		for (int i = 1; i < hi; i++) {
			Recording[] temp = highestDiffFromIndex(sortedByTime, i);
			if (difference(temp) > difference(res)){
				res = temp;
			}
		}

		return res;
	}

	private int difference(Recording[] tuple){
		return Math.abs(tuple[0].temp - tuple[1].temp);
	}

	private Recording[] highestDiffFromIndex(Recording[] arr, int lo){
		long start = arr[lo].time.getTime();

		int hi = lo;
		int elapsed = (int)(start - arr[hi].time.getTime());
		// find hi
		while(elapsed <= 160){
			hi++;
			if(hi >= arr.length) break;
			elapsed = (int)(start - arr[hi].time.getTime());
		}
		hi--; // go back one step

		Recording[] tempArr = new Recording[hi-lo];
		for (int i = 0; i < (hi-lo); i++) {
			tempArr[i] = arr[lo+i];
		}
		Arrays.sort(tempArr, new Comparator<Recording>() {
			@Override
			public int compare(Recording o1, Recording o2) {
				return Integer.compare(o1.temp, o2.temp);
			}
		});

		Recording[] pair = new Recording[2];
		pair[0] = tempArr[0]; // lowest
		pair[1] = tempArr[hi-lo-1]; // highest

		return pair;
	}

	// assumes that the list contains at least 5 elements otherwise it crashes
	private Recording[] highest5(Recording[] sortedList){
		Recording[] highest = new Recording[5];
		int j = 4;
		for (int i = sortedList.length-1; i > sortedList.length-6 ; i--) {
			highest[j] = sortedList[i];
			j--;
		}
		return highest;
	}

	private Recording[] lowest5(Recording[] sortedList){
		Recording[] lowest = new Recording[5];
		for (int i = 0; i < 5; i++) {
			lowest[i] = sortedList[i];
		}
		return lowest;
	}

	private Recording[] list2arr(LinkedList<Recording> list){
		Recording[] arr = new Recording[list.size()];

		int i = 0;
		for (Iterator<Recording> iter = list.iterator(); iter.hasNext();){
			Recording ele = iter.next();
			arr[i] = ele;
			i++;
		}

		return arr;
	}
}

/**
 * An object for a single record.
 */
class Recording implements Comparable<Recording>{
	int temp;
	Timestamp time;
	final static Timestamp start = new Timestamp(System.currentTimeMillis());

	Recording(int temp){
		this.temp = temp;
		time = new Timestamp(System.currentTimeMillis());
	}

	/**
	 * the default comparison is on "time"
	 * @param o object to compare to
	 * @return standard comparison format
	 */
	@Override
	public int compareTo(Recording o) {
		return Long.compare(this.time.getTime(), o.time.getTime());
	}

	public String toString(){
		return temp + "";
//		return (time.getTime() - start.getTime()) + ""; // print the timestamps
	}

	/**
	 * generates a random temperature within the specified range
	 * @param min min value
	 * @param max max value
	 * @return rand integer from min to max (exclusive)
	 */
	public static int randTemp(int min, int max){
		return (int)(Math.random() * (max - min) + min);
	}
}

/**
 * Wait-free queue shared by an Ant and the Queen
 * This is a two-thread wait-free queue.
 * @param <T> type of the queue
 */
class WaitFreeQueue<T> {
	int head = 0;
	int tail = 0;
	int capacity;
	T[] items;

	WaitFreeQueue(int capacity){
		this.capacity = capacity;
		items = (T[]) new Object[capacity];
	}

	/**
	 * adds an element to the queue
	 * linearization is at the Exception or somewhere between tail++
	 * @param x element to add
	 * @throws Exception full exception
	 */
	public void enq(T x) throws Exception {
		if(tail - head == capacity)
			throw new Exception("ERROR: trying to enqueue to a full queue");
		items[tail % capacity] = x;
		tail++; // linearization point
	}

	/**
	 * removes an element from the queue
	 * linearization point is at the Exception or somewhere between head++
	 * @return item removed
	 * @throws Exception empty exception
	 */
	public T deq() throws Exception {
		if(tail == head)
			throw new Exception("ERROR: trying to dequeue from an empty queue");
		T item = items[head % capacity];
		head++; // linearization point
		return item;
	}

	public boolean isEmpty(){
		return (head - tail) == 0;
	}

	/**
	 * used for debugging, prints the entire queue
	 * it constructs a string and prints the entire string at once to avoid weird outputs due to interrupts
	 * @param threadId the thread that requests the printing
	 * @throws Exception empty queue
	 */
	public void printQueue(String threadId) throws Exception {
		String ans = "";
		ans += threadId + ": ";
		while(!this.isEmpty()){
			ans += this.deq() + ", ";
		}
		System.out.println(ans);
	}
}