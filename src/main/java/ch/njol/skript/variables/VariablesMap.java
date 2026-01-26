package ch.njol.skript.variables;

import ch.njol.skript.lang.Variable;
import ch.njol.util.StringUtils;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.errorprone.annotations.ThreadSafe;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Unmodifiable;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;

/**
 * A thread-safe, memory-efficient Radix Tree for storing variables.
 */
@ThreadSafe
public final class VariablesMap {

	private static final Comparator<String> VARIABLE_NAME_COMP = (s1, s2) -> {
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

	private final Node root = new Node();
	private final LoadingCache<String, Optional<Object>> cache = CacheBuilder.newBuilder()
		.maximumSize(10_000)
		.expireAfterAccess(10, TimeUnit.MINUTES)
		.weigher((Weigher<String, Optional<Object>>) (key, value) -> {
			if (value.isEmpty())
				return 0;
			if (value.get() instanceof Map<?,?> map)
				return map.size();
			return 1;
		})
		.build(CacheLoader.from(this::getVariableOpt));

	/**
	 * A node in the radix tree.
	 */
	private static class Node {
		final StampedLock lock = new StampedLock();
		@Nullable Object value;
		@Nullable TreeMap<String, Node> children;

		boolean hasChildren() {
			return children != null && !children.isEmpty();
		}

		boolean isEmpty() {
			return value == null && !hasChildren();
		}
	}

	/**
	 * Returns the internal value of the requested variable.
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
		return cache.getUnchecked(name).orElse(null);
	}

	public Optional<Object> getVariableOpt(String name) {
		boolean isList = name.endsWith(Variable.SEPARATOR + "*");
		if (isList)
			name = name.substring(0, name.length() - (Variable.SEPARATOR.length() + 1)); // strip the "::*" suffix

		String[] parts = Variables.splitVariableName(name);
		Node current = root;

		// iterate through the variable parts
		for (String part : parts) {
			Node lockedNode = current;
			long stamp = lockedNode.lock.readLock();
			try {
				if (!lockedNode.hasChildren()) return Optional.empty();
				assert lockedNode.children != null;
				Node next = lockedNode.children.get(part);
				if (next == null) return Optional.empty();
				current = next;
			} finally {
				lockedNode.lock.unlockRead(stamp);
			}
		}

		if (isList) {
			// map the radix node to a sorted map
			long stamp = current.lock.readLock();
			try {
				if (!current.hasChildren()) return Optional.empty();
				assert current.children != null;
				Map<String, Object> map = new TreeMap<>(VARIABLE_NAME_COMP);
				current.children.forEach((key, child) -> {
					if (child.isEmpty()) return;
					if (child.value != null && !child.hasChildren()) {
						map.put(key, child.value);
					} else {
						map.put(key, asTreeMap(child));
					}
				});
				return map.isEmpty() ? Optional.empty() : Optional.of(map);
			} finally {
				current.lock.unlockRead(stamp);
			}
		} else {
			// read the value, first try optimistic, if fails acquire the lock
			long stamp = current.lock.tryOptimisticRead();
			Object val = current.value;
			if (!current.lock.validate(stamp)) {
				stamp = current.lock.readLock();
				try {
					val = current.value;
				} finally {
					current.lock.unlockRead(stamp);
				}
			}
			return Optional.ofNullable(val);
		}
	}

	private TreeMap<String, Object> asTreeMap(Node node) {
		TreeMap<String, Object> map = new TreeMap<>(VARIABLE_NAME_COMP);
		if (node.value != null) map.put(null, node.value);
		if (node.hasChildren()) {
			assert node.children != null;
			node.children.forEach((key, child) -> map.put(key, asTreeMap(child)));
		}
		return map;
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

		// set variable before updating the cache
		setVariable(root, parts, 0, value, isList);

		// invalidate the exact key
		cache.invalidate(name);

		// invalidate all parents
		if (!isList) {
			StringBuilder buffer = new StringBuilder();
			for (int i = 0; i < parts.length - 1 /* -1 because the exact key is invalidated before */; i++) {
				if (i > 0)
					buffer.append(Variable.SEPARATOR);
				buffer.append(parts[i]);
				cache.invalidate(buffer + Variable.SEPARATOR + "*");
			}
		}

		// invalidate children
		if (isList) {
			String prefix = actualName + Variable.SEPARATOR;
			cache.asMap().keySet().removeIf(k -> k.startsWith(prefix));
		}
	}

	private boolean setVariable(Node current, String[] parts, int index, @Nullable Object value, boolean isListClear) {
		long stamp = current.lock.writeLock();
		try {
			// reached target node
			if (index == parts.length) {
				if (isListClear) {
					current.children = null; // clear the children
				} else {
					current.value = value;
				}
				return current.isEmpty();
			}

			if (current.children == null) {
				if (value == null)
					return current.isEmpty();
				current.children = new TreeMap<>(VARIABLE_NAME_COMP);
			}

			String key = parts[index];
			Node child = current.children.get(key);
			if (child == null) {
				if (value == null)
					return current.isEmpty();
				child = new Node();
				current.children.put(key, child);
			}

			boolean prune = setVariable(child, parts, index + 1, value, isListClear);

			// do not store empty nodes that lead to nothing
			if (prune) {
				current.children.remove(key);
				if (current.children.isEmpty())
					current.children = null;
			}

			return current.isEmpty();
		} finally {
			current.lock.unlockWrite(stamp);
		}
	}

	/**
	 * Creates a copy of this map.
	 * <p>
	 * This method is strongly consistent and returns a copy of a snapshot of the variables map at
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
				target.children = new TreeMap<>(VARIABLE_NAME_COMP);
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
	 * This method is strongly consistent and returns a full snapshot of the variables map at
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
	 * This method is strongly consistent and returns number of variables at
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
