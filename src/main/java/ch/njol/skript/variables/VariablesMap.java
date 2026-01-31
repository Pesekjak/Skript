package ch.njol.skript.variables;

import ch.njol.skript.lang.Variable;
import ch.njol.util.StringUtils;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.ThreadSafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;

/**
 * A thread-safe, memory-efficient Radix Tree for storing variables.
 */
@ThreadSafe
public final class VariablesMap {

	/**
	 * Comparator for variable names.
	 */
	public static final Comparator<String> VARIABLE_NAME_COMP = (s1, s2) -> {
		if (s1 == null)
			return s2 == null ? 0 : -1;
		if (s2 == null)
			return 1;

		int i = 0;
		int j = 0;

		boolean lastNumberNegative = false;
		boolean afterDecimalPoint = false;

		while (i < s1.length() && j < s2.length()) {
			char c1 = s1.charAt(i);
			char c2 = s2.charAt(j);

			// Numbers/digits are treated differently from other characters.
			if (Character.isDigit(c1) && Character.isDigit(c2)) {

				// The index after the last digit
				int end1 = StringUtils.findLastDigit(s1, i);
				int end2 = StringUtils.findLastDigit(s2, j);

				// Amount of leading zeroes
				int leadingZeros1 = 0;
				int leadingZeros2 = 0;

				if (!afterDecimalPoint) {
					while (i < end1 - 1 && s1.charAt(i) == '0') {
						i++;
						leadingZeros1++;
					}
					while (j < end2 - 1 && s2.charAt(j) == '0') {
						j++;
						leadingZeros2++;
					}
				}

				// If the number is prefixed by a '-', it should be treated as negative, thus inverting the order.
				// If the previous number was negative, and the only thing separating them was a '.',
				//  then this number should also be in inverted order.
				int startOfNumber = i - leadingZeros1;
				boolean currentIsNegative = startOfNumber > 0 && s1.charAt(startOfNumber - 1) == '-';

				// if the previous number was negative and we just crossed a dot, we stay negative
				boolean effectiveNegative = currentIsNegative || lastNumberNegative;
				int sign = effectiveNegative ? -1 : 1;

				int length1 = end1 - i;
				int length2 = end2 - j;

				// Different length numbers (99 > 9)
				if (!afterDecimalPoint && length1 != length2)
					return (length1 - length2) * sign;

				// Iterate over the digits
				while (i < end1 && j < end2) {
					int diff = s1.charAt(i) - s2.charAt(j);
					if (diff != 0)
						return diff * sign;
					i++;
					j++;
				}

				// Different length numbers (1.99 > 1.9)
				if (afterDecimalPoint && length1 != length2)
					return (length1 - length2) * sign;

				// If the numbers are equal, but either has leading zeroes,
				//  more leading zeroes is a lesser number (01 < 1)
				if (leadingZeros1 != leadingZeros2)
					return (leadingZeros1 - leadingZeros2) * sign;

				// We finished processing a number, we are now "after" a number.
				// If the next char is a dot, we remain in decimal mode.
				afterDecimalPoint = true;
				// this is for backwards compatibility, else it should be effectiveNegative
				lastNumberNegative = currentIsNegative;
			}
			// Normal characters
			else {
				if (c1 != c2)
					return c1 - c2;

				// Reset the last number flags if we're exiting a number.
				if (c1 != '.') {
					lastNumberNegative = false;
					afterDecimalPoint = false;
				}

				i++;
				j++;
			}
		}

		// One is prefix of the other
		if (i < s1.length())
			return lastNumberNegative ? -1 : 1;
		if (j < s2.length())
			return lastNumberNegative ? 1 : -1;
		return 0;
	};

	/**
	 * A node in the radix tree.
	 */
	private static class Node {
		final StampedLock lock = new StampedLock();
		@Nullable Object value;
		@Nullable Map<String, Node> children;

		/**
		 * @return whether the node has children
		 */
		boolean hasChildren() {
			return children != null && !children.isEmpty();
		}

		/**
		 * @return whether the node is empty (has no value and no children)
		 */
		boolean isEmpty() {
			return value == null && !hasChildren();
		}

		/**
		 * Unlocks the lock with given stamp.
		 * <p>
		 * Works for both read and write locks.
		 *
		 * @param stamp lock stamp
		 */
		void unlock(long stamp) {
			if (StampedLock.isWriteLockStamp(stamp)) {
				lock.unlockWrite(stamp);
			} else {
				lock.unlockRead(stamp);
			}
		}
	}

	/**
	 * Root node of the tree.
	 */
	private final Node root = new Node();

	/**
	 * Estimate of empty branches in the radix tree.
	 * <p>
	 * The real number may be different as some branches may be populated after clear.
	 */
	private final AtomicInteger leftEmpty = new AtomicInteger(0);

	/**
	 * At how many writes that leave empty branches {@link #prune()} should be executed.
	 */
	private final int pruneAt;

	/**
	 * Executor of automatic prune operation.
	 */
	private final Executor pruneExecutor;

	/**
	 * Constructs new variables map that automatically calls {@link #prune()}
	 * after certain number of {@link #setVariable(String, Object)} left
	 * empty branches in the radix tree.
	 *
	 * @param pruneAt after which number of such writes the variables map should call prune
	 * @param pruneExecutor executor which will execute the expensive prune operation
	 */
	public VariablesMap(int pruneAt, Executor pruneExecutor) {
		this.pruneAt = pruneAt;
		this.pruneExecutor = pruneExecutor;
	}

	/**
	 * Constructs new variables map that automatically calls {@link #prune()}
	 * after certain number of {@link #setVariable(String, Object)} left
	 * empty branches in the radix tree.
	 *
	 * @param pruneAt after which number of such writes the variables map should call prune
	 */
	public VariablesMap(int pruneAt) {
		this(pruneAt, Runnable::run);
	}

	/**
	 * Constructs new variables map.
	 *
	 * @see #prune()
	 */
	public VariablesMap() {
		this(Integer.MAX_VALUE, Runnable::run);
	}

	/**
	 * Returns the value of the requested variable.
	 * <p>
	 * In case of list variables, the returned map is not the backing map
	 * of the variables map and can be edited safely (is modifiable).
	 * <p>
	 * If map is returned, it is sorted using a comparator that matches the variable name sorting.
	 * <p>
	 * If map is returned the structure is as following:
	 * <ul>
	 *     <li>
	 *         If value is present for the variable and
	 *         <ul>
	 *             <li>the variable has no children, it is mapped directly to the key</li>
	 *             <li>the variable has children it is mapped to a map, that maps {@code null} to its value and its
	 *             children are mapped using the same strategy</li>
	 *         </ul>
	 *     </li>
	 *     <li>If value is not present for the variable, it is mapped to a map with its children mapped using the same
	 *     strategy</li>
	 * </ul>
	 *
	 * @param name the name of the variable, possibly a list variable.
	 * @return an {@link Object} for a normal variable or a
	 * {@code Map<String, Object>} for a list variable,
	 * or {@code null} if the variable is not set.
	 */
	public @Nullable Object getVariable(String name) {
		boolean isList = name.endsWith(Variable.SEPARATOR + "*");
		if (isList)
			name = name.substring(0, name.length() - (Variable.SEPARATOR.length() + 1)); // strip the "::*" suffix

		String[] parts = Variables.splitVariableName(name);
		Node current = root;
		long stamp = current.lock.readLock();

		try {
			for (String part : parts) {
				if (!current.hasChildren())
					return null;
				assert current.children != null;
				Node next = current.children.get(part);
				if (next == null)
					return null;

				long nextStamp = next.lock.readLock();
				current.lock.unlockRead(stamp);

				current = next;
				stamp = nextStamp;
			}

			if (isList) {
				if (!current.hasChildren())
					return null;
				assert current.children != null;
				Map<String, Object> map = new TreeMap<>(VARIABLE_NAME_COMP);
				current.children.forEach((key, child) -> {
					Object resolved = resolve(child);
					if (resolved != null)
						map.put(key, resolved);
				});

				return map.isEmpty() ? null : map;
			} else {
				return current.value;
			}
		} finally {
			current.lock.unlockRead(stamp);
		}
	}

	/**
	 * Converts the node into its object representation.
	 * <p>
	 * That is either a TreeMap or its value if it has no children.
	 *
	 * @param node node
	 * @return node as object
	 */
	private Object resolve(Node node) {
		long stamp = node.lock.readLock();
		try {
			if (node.isEmpty())
				return null;
			if (!node.hasChildren())
				return node.value;
			TreeMap<String, Object> map = new TreeMap<>(VARIABLE_NAME_COMP);
			if (node.value != null)
				map.put(null, node.value);
			if (node.hasChildren()) {
				assert node.children != null;
				node.children.forEach((key, child) -> {
					Object resolved = resolve(child);
					if (resolved != null)
						map.put(key, resolved);
				});
			}
			return map.isEmpty() ? null : map;
		} finally {
			node.lock.unlockRead(stamp);
		}
	}

	/**
	 * Sets the given variable to the given value.
	 * <p>
	 * This method accepts list variables,
	 * but these may only be set to {@code null}.
	 *
	 * @param name the variable name.
	 * @param value the variable value, {@code null} to delete the variable.
	 */
	public void setVariable(String name, @Nullable Object value) {
		boolean isList = name.endsWith(Variable.SEPARATOR + "*");

		String actualName = isList
			? name.substring(0, name.length() - (Variable.SEPARATOR.length() + 1))
			: name;

		if (isList) {
			Preconditions.checkState(value == null, "List variables can only be set to null");
		}

		String[] parts = Variables.splitVariableName(actualName);

		if (value == null) {
			clearVariable(root, parts, isList);
		} else {
			setSingleVariable(root, parts, value);
		}
	}

	/**
	 * Optimized iterative setter for single non-null values.
	 * <p>
	 * Traversals use read locks, write lock is only acquired at the specific node
	 * that needs modification.
	 * <p>
	 * This is possible because prune does not happen when setting non-null values (the
	 * parent nodes are not modified on the way back)
	 */
	private void setSingleVariable(Node root, String[] parts, Object value) {
		Node current = root;
		long stamp = current.lock.readLock();
		try {
			for (int i = 0; i < parts.length; i++) {
				String key = parts[i];
				boolean isLast = i == parts.length - 1;

				// we need write lock if the child does not exist (to create the node in the map)
				boolean childExists = current.children != null && current.children.containsKey(key);

				if (!childExists) {
					// try to upgrade
					long ws = current.lock.tryConvertToWriteLock(stamp);
					if (ws == 0L) {
						// if failed, reverse and wait for write lock
						current.lock.unlockRead(stamp);
						stamp = current.lock.writeLock();
					} else {
						stamp = ws;
					}
					if (current.children == null)
						current.children = new HashMap<>();
					// could already be added during the waiting on the write lock
					current.children.putIfAbsent(key, new Node());
				}

				assert current.children != null;
				Node next = current.children.get(key);
				// for last node we write the value
				long nextStamp = isLast ? next.lock.writeLock() : next.lock.readLock();

				current.unlock(stamp);

				current = next;
				stamp = nextStamp;

				if (isLast) {
					current.value = value; // we have write lock
					return;
				}
			}
		} finally {
			// unlock the final node
			current.unlock(stamp);
		}
	}

	/**
	 * Iterative setter for clearing values.
	 */
	private void clearVariable(Node root, String[] parts, boolean isListClear) {
		Node current = root;
		long stamp = current.lock.readLock();
		boolean prune = false;
		try {
			for (int i = 0; i < parts.length; i++) {
				String key = parts[i];
				boolean isLast = i == parts.length - 1;

				// if child does not exist there is nothing to clear
				if (current.children == null || !current.children.containsKey(key))
					return; // unlocks in the finally block

				Node next = current.children.get(key);
				// for last node we write the value
				long nextStamp = isLast ? next.lock.writeLock() : next.lock.readLock();

				current.unlock(stamp);

				current = next;
				stamp = nextStamp;

				if (isLast) {
					if (isListClear) {
						current.children = null;
					} else {
						current.value = null;
					}

					// clear may caused empty branches in the tree
					if (current.isEmpty()) {
						int count = leftEmpty.incrementAndGet();
						// automatic prune call
						if (count >= pruneAt) {
							if (leftEmpty.compareAndSet(count, 0))
								prune = true;
						}
					}
					return;
				}
			}
		} finally {
			current.unlock(stamp);
			if (prune)
				pruneExecutor.execute(this::prune);
		}
	}

	/**
	 * Prunes the entire tree, removing all empty nodes.
	 * <p>
	 * This operation is expensive and fully write locks the radix tree.
	 */
	public void prune() {
		prune(root);
	}

	private boolean prune(Node node) {
		long stamp = node.lock.writeLock();
		try {
			if (node.isEmpty())
				return true;

			assert node.children != null;
			var it = node.children.entrySet().iterator();
			while (it.hasNext()) {
				var entry = it.next();
				boolean isChildEmpty = prune(entry.getValue());
				if (isChildEmpty)
					it.remove();
			}

			if (node.children.isEmpty())
				node.children = null;

			return node.isEmpty();
		} finally {
			node.lock.unlockWrite(stamp);
		}
	}

	/**
	 * Creates a copy of this map.
	 * <p>
	 * This method returns a copy of a snapshot of the variables map at
	 * the current moment.
	 *
	 * @return the copy.
	 */
	public VariablesMap copy() {
		VariablesMap copy = new VariablesMap();
		copy(this.root, copy.root);
		return copy;
	}

	private void copy(Node source, Node target) {
		long stamp = source.lock.readLock();
		try {
			target.value = source.value;
			if (source.hasChildren()) {
				assert source.children != null;
				target.children = new HashMap<>();
				source.children.forEach((key, sourceChild) -> {
					Node targetChild = new Node();
					copy(sourceChild, targetChild);
					target.children.put(key, targetChild);
				});
			}
		} finally {
			source.lock.unlockRead(stamp);
		}
	}

	/**
	 * @return whether the variables map is empty
	 */
	public boolean isEmpty() {
		return root.isEmpty();
	}

	/**
	 * Returns all variables in this map.
	 * <p>
	 * The map is no guaranteed order and may not be modifiable.
	 * <p>
	 * This method returns a full snapshot of the variables map at
	 * the current moment.
	 * <p>
	 * This map is not nested and contains variables in format {@code full key <-> value}
	 *
	 * @return all variables in this map
	 */
	public @Unmodifiable Map<String, Object> getAll() {
		Map<String, Object> all = new TreeMap<>(VARIABLE_NAME_COMP);
		getAll("", root, all::put);
		return Collections.unmodifiableMap(all);
	}

	private void getAll(String buffer, Node source, BiConsumer<String, Object> collector) {
		long stamp = source.lock.readLock();
		try {
			if (source.value != null)
				collector.accept(buffer, source.value);
			if (source.hasChildren()) {
				assert source.children != null;
				source.children.forEach((key, child) -> {
					String nextName = buffer.isEmpty() ? key : buffer + Variable.SEPARATOR + key;
					getAll(nextName, child, collector);
				});
			}
		} finally {
			source.lock.unlockRead(stamp);
		}
	}

	/**
	 * Returns number of variables in this map.
	 * <p>
	 * This method returns number of variables at
	 * the current moment.
	 *
	 * @return number of variables in this map
	 */
	public long size() {
		return size(root);
	}

	private long size(Node node) {
		long stamp = node.lock.readLock();
		long size = 0;
		try {
			if (node.value != null)
				size++;
			if (node.hasChildren()) {
				assert node.children != null;
				for (Node child : node.children.values())
					size += size(child);
			}
		} finally {
			node.lock.unlockRead(stamp);
		}
		return size;
	}

}
