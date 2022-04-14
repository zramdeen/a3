/*=============================================================================
| Assignment: The Birthday Presents Party.
|
| Author: Zahid Ramdeen
| Language: Java
|
| To Compile: (from terminal)
| javac P1.java
|
| To Execute: (from terminal)
| java P1
|
| Customize: (optional)
| 	modify the "totalPresents" variable in the program
| 	modify the "threshold" variable in the Servant class.
|
| Class: COP4520 - Concepts of Parallel and Distributed Processing - Spring 2022
| Instructor: Damian Dechev
| Due Date: 4/13/2022
|
+=============================================================================*/


import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.atomic.AtomicMarkableReference;

public class P1 {
	public static void main(String[] args) {
		NBLinkedList<Integer> outList = new NBLinkedList<>(Integer.MIN_VALUE, Integer.MAX_VALUE);

		// unordered bag of presents
		int totalPresents = 500000;
		ArrayList<Present> inBag = new ArrayList<>();
		for (int i = 0; i < totalPresents; i++) {
			inBag.add(new Present(i));
		}
		Collections.shuffle(inBag);
//		System.out.println(inBag.toString()); // initial state --- enable with small lists

		// create the threads
		// fork-join setup
		ForkJoinPool pool = new ForkJoinPool(1);
		NBLinkedList<Integer> res = pool.invoke(new ServantTask(inBag, 0, totalPresents));
//		res.printList(); // sorted state --- enable with small lists
		System.out.println("done");

//		System.out.println(inBag.toString());
	}
}

/**
 * Represents a present.
 * id = unique integer
 * marked = processed state
 */
class Present {
	int id;
	boolean marked;

	Present(int id){
		this.id = id;
		marked = false;
	}

	public String toString(){
		return id + "" /*+ marked*/;
	}
}

/**
 * Thread for a servant.
 * The servant is responsible for
 */
class ServantTask extends RecursiveTask<NBLinkedList<Integer>> {
	int lo;
	int hi;
	ArrayList<Present> inBag;
	ThreadLocal<NBLinkedList<Integer>> outList;
	static int THRESHOLD = 1001;


	public ServantTask(ArrayList<Present> inBag, int lo, int hi){
		this.inBag = inBag;
		this.outList = new ThreadLocal<NBLinkedList<Integer>>(){
			@Override public NBLinkedList<Integer> initialValue(){
				return new NBLinkedList<>(Integer.MIN_VALUE, Integer.MAX_VALUE);
			}
		};
		this.lo = lo;
		this.hi = hi;
	}

	@Override
	protected NBLinkedList<Integer> compute() {
		int target = hi - lo;

		// sequential execution
		if(target < THRESHOLD){
			int tyNotes = 0; // track the ty notes.
			ServantJob job = ServantJob.randomTask();

			// perform arbitrary job
			while(tyNotes < target){
				switch(job){
					case COPY -> { // includes the RESPONSE
						inBag.get(lo + tyNotes).marked = true; // mark as deleted
						outList.get().add(inBag.get(lo + tyNotes).id); // add the present
						tyNotes++; // create the ty letter
					}
					case SEARCH -> {
						outList.get().contains((int)(Math.random() * (hi - lo) + lo)); // dont actually care about ans
					}
					default -> System.out.println("something went wrong in switch");
				}
				job = ServantJob.randomTask(); // get new job
			}

			return outList.get();

		} else { // create more tasks.
			int mid = (lo + hi) / 2;
			ServantTask left = new ServantTask(inBag, lo, mid);
			ServantTask right = new ServantTask(inBag, mid, hi);
			right.fork();
			NBLinkedList<Integer> leftRes = left.compute();
			NBLinkedList<Integer> rightRes = right.join();

			// print states
//			leftRes.printList(); // use with small lists
//			rightRes.printList(); // use with small lists

			NBLinkedList<Integer> ans = new NBLinkedList<>(Integer.MIN_VALUE, Integer.MAX_VALUE);
			leftRes.mergeLists(rightRes, ans);
			return ans;
		}
	}
}

/**
 * Textbook non-blocking linked list verbatim.
 * This list allows for a wait-free contains method.
 * The add() and remove() functions are lock-free.
 * @param <T> type of the node
 */
class NBLinkedList<T> {
	Node<T> head;
	Node<T> tail; // may not need

	public NBLinkedList(T startSentinel, T endSentinel){
		head = new Node<T>(startSentinel);
		tail = new Node<T>(endSentinel);
		head.next.set(tail, false);
	}

	/**
	 * lock free add method
	 * @param item element to add to the list
	 * @return true if successful
	 */
	public boolean add(T item){
		int key = item.hashCode();
		while(true){
			Window<T> window = find(head, key);
			Node<T> pred = window.pred;
			Node<T> curr = window.curr;

			// add regardless if node exists in the list or not
			Node<T> node = new Node(item);
			node.next.set(curr, false); // node points to current and unmarked
			if(pred.next.compareAndSet(curr, node, false, false))
				return true; // pred successfully links to new node
		}
	}

	/**
	 * lock free remove method
	 * @param item removes element from the list if it exists
	 * @return false if it does not exist
	 */
	public boolean remove(T item){
		int key = item.hashCode();
		boolean snip;
		while(true){
			Window<T> window = find(head, key);
			Node<T> pred = window.pred;
			Node<T> curr = window.curr;
			if(curr.key != key)
				return false; // not in the list
			else {
				Node<T> succ = curr.next.getReference();
				snip = curr.next.compareAndSet(succ, succ,false,false);
				if(!snip) // snip is logical deletion
					continue; // retry loop
				pred.next.compareAndSet(curr, succ, false, false);
				return true; // if phys deletion fails just defer to next method call
			}
		}
	}

	/**
	 * wait-free contains method
	 * @param item element to search for
	 * @return true if found
	 */
	public boolean contains(T item){
		boolean[] marked = {false};
		int key = item.hashCode();
		Node<T> curr = head;
		while(curr.key < key){
			curr = curr.next.getReference();
			curr.next.get(marked); // just used to get marked value
		}
		return (curr.key == key && !marked[0]);
	}

	/**
	 * use for debugging
	 * only print small lists for obvious reasons
	 */
	public void printList(){
		Node<T> curr = head.next.getReference();
		System.out.print("[");
		while(null != curr && curr.key < Integer.MAX_VALUE){
			if(curr.next.getReference().key == Integer.MAX_VALUE)
				System.out.print(curr.key + "]");
			else
				System.out.print(curr.key + ", ");
			curr = curr.next.getReference();
		}
		System.out.println();
	}

	/**
	 * navigate list and find the pred, curr node for given key
	 * @param head start of the sorted list
	 * @param key index in the list
	 * @return pair of pred and curr called Window
	 */
	public Window<T> find(Node<T> head, int key){
		Node<T> pred = null;
		Node<T> curr = null;
		Node<T> succ = null;
		boolean[] marked = {false}; // array of length 1... java hack
		boolean snip;
		retry: while(true){
			pred = head;
			curr = pred.next.getReference();
			while(true){ // remove marked nodes
				succ = curr.next.get(marked);
				while(marked[0]){
					snip = pred.next.compareAndSet(curr, succ, false, false);
					if(!snip) continue retry;
					curr = succ;
					succ = curr.next.get(marked);
				}
				if(curr.key >= key) // desired location
					return new Window(pred, curr);
				pred = curr;
				curr = succ;
			}
		}
	}

	/**
	 * As linked lists are terrible at maintaining sorted list, use divide-and-conquer.
	 * This is a helpful function to facilitate merging two lists.
	 * @param B second list to merge, the first being "this"
	 * @param ans merged list
	 */
	public void mergeLists(NBLinkedList<T> B, NBLinkedList<T> ans){
		NBLinkedList<T> A = this;
		ArrayList<T> temp = new ArrayList<>();
		int i = 0;

		Node<T> currA = A.head.next.getReference();
		Node<T> currB = B.head.next.getReference();

		while(null != currA || null != currB) {
			T item;

			// insert rest of B
			if(null == currA){
				while(currB.key < Integer.MAX_VALUE){
					item = currB.item;
					temp.add(i, item);
					i++;
					currB = currB.next.getReference();
				}
				break;
			}

			// insert rest of A
			if(null == currB){
				while(currA.key < Integer.MAX_VALUE){
					item = currA.item;
					temp.add(i, item);
					i++;
					currA = currA.next.getReference();
				}
				break;
			}

			// select lowest
			if(currA.key < currB.key){
				item = currA.item;
				currA = currA.next.getReference();
			} else {
				item = currB.item;
				currB = currB.next.getReference();
			}

			temp.add(i, item);
			i++;
		}

		// insert to the new list in reverse order
		i--; i--; // extra i-- to remove the trailing sentinel node.
		while(i >= 0){
			ans.add(temp.get(i));
			i--;
		}
	}
}

/**
 * Window object from textbook verbatim
 * @param <T> type of the node
 */
class Window<T> {
	public Node<T> pred;
	public Node<T> curr;
	Window(Node<T> pred, Node<T> curr){
		this.pred = pred;
		this.curr = curr;
	}
}

/**
 * Node from the textbook verbatim
 * @param <T> type of the node
 */
class Node<T>{
	T item; // can be null tbh, we only care about id
	int key; // unique id
	AtomicMarkableReference<Node<T>> next;

	Node(T item){
		this.key = item.hashCode();
		this.item = item;
		this.next = new AtomicMarkableReference<>(null, false);
	}

	public String toString(){
		return key + "";
	}
}

/**
 * Enum used to generate jobs for the servant.
 */
enum ServantJob {
	COPY, // from unordered bag -> linked list
	SEARCH, // search linked list for item
	RESPONSE; // thank you to guest

	private static final List<ServantJob> VALUES = Collections.unmodifiableList(Arrays.asList(values()));
	private static final int SIZE = VALUES.size();
	private static final Random RANDOM = new Random();

	/**
	 * As per the minotaur's requirement it only gives "COPY" or "SEARCH"
	 * @return random job for the servant
	 */
	public static ServantJob randomTask(){
		return VALUES.get(RANDOM.nextInt(SIZE-1));
	}
}
