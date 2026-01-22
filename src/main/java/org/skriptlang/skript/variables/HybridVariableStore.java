package org.skriptlang.skript.variables;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.fory.memory.MemoryBuffer;

import ch.njol.skript.Skript;
import org.jetbrains.annotations.NotNull;
import org.skriptlang.skript.variables.fory.ForyContext;

/**
 * A high-performance variable storage using a 2 layer architecture.
 * <p>
 * L1 Cache (Heap): Variables that have been recently accessed are stored in a Caffeine cache.
 * <br>
 * L2 Store (Off-Heap): Variables are serialized into native memory using FFM API, see {@link OffHeapSlabManager}.
 * <p>
 * This class only handles lookup for the variables values by keys and allows large amount of variables to be
 * effectively stored in memory at runtime.
 */
@ThreadSafe
public class HybridVariableStore implements AutoCloseable {

	private static final long COMPACTION_THRESHOLD = 64 * 1024 * 1024; // compaction when 64MB of dead data accumulates

	private final LoadingCache<@NotNull String, Object> l1Cache = Caffeine.newBuilder()
		.maximumSize(100_000)
		.executor(Runnable::run)
		.expireAfterAccess(Duration.ofMinutes(5))
		.removalListener(this::updateOffHeap)
		.build(this::readFromOffHeap);
	private Map<String, SlabPointer> l2Index = new ConcurrentHashMap<>();

	private final ForyContext fory;

	// locks write when compacting the off-heap
	private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
	private OffHeapSlabManager slabManager = new OffHeapSlabManager();

	private final AtomicLong wastedBytes = new AtomicLong(0);
	private volatile boolean isCompacting = false;

	private record PendingAction(Object value, boolean isDeletion) {}
	private final Map<String, PendingAction> pendingUpdates = new ConcurrentHashMap<>();

	public HybridVariableStore(ForyContext fory) {
		this.fory = fory;
	}

	/**
	 * Persists a variable to off-heap memory. Called automatically by cache eviction.
	 * <p>
	 * Off-heap does not always contain the current value of the variable but cache does.
	 * When the cache is invalided or evicted, the value of the variable is persisted in the off-heap.
	 *
	 * @param key key of the variable
	 * @param value previous value
	 */
	private void updateOffHeap(String key, Object value, RemovalCause removalCause) {
		if (removalCause == RemovalCause.REPLACED) // new value is safely in heap
			return;
		lifecycleLock.readLock().lock();
		try {
			if (isCompacting) {
				boolean isDelete = removalCause == RemovalCause.EXPLICIT;
				pendingUpdates.put(key, new PendingAction(value, isDelete));
				return;
			}

			if (removalCause == RemovalCause.EXPLICIT) { // variable is cleared
				SlabPointer removed = l2Index.remove(key);
				if (removed != null) wastedBytes.addAndGet(removed.length());
				return;
			}
			if (removalCause.wasEvicted()) { // automatic due to eviction, needs to be persisted to the off-heap
				l2Index.compute(key, (_, oldPtr) -> {
					MemoryBuffer buffer = fory.serialize(value);
					if (oldPtr != null) {
						MemoryBuffer oldBuffer = slabManager.read(oldPtr);
						if (oldBuffer != null && convert(buffer).equals(convert(oldBuffer)))
							return oldPtr;
						wastedBytes.addAndGet(oldPtr.length());
					}
					return slabManager.write(buffer);
				});
			}
		} finally {
			lifecycleLock.readLock().unlock();
		}

		if (wastedBytes.get() > COMPACTION_THRESHOLD) {
			triggerCompaction();
		}
	}

	/**
	 * Loads variable from off-heap memory.
	 * <p>
	 * This is called when caching the variable value and is thread safe by the design of Caffeine's cache.
	 *
	 * @param key key of the variable
	 * @return value, null if the variable is not set
	 */
	private @Nullable Object readFromOffHeap(String key) {
		lifecycleLock.readLock().lock();
		try {
			SlabPointer ptr = l2Index.get(key);
			if (ptr == null) return null;

			MemoryBuffer buffer = slabManager.read(ptr);
			if (buffer == null) return null;

			return fory.deserialize(buffer);
		} finally {
			lifecycleLock.readLock().unlock();
		}
	}

	/**
	 * Returns the value of variable with given key.
	 *
	 * @param key variable key
	 * @return variable value or null if not set
	 */
	public @Nullable Object get(String key) {
		return l1Cache.get(key);
	}

	/**
	 * Updates the value of variable with given key.
	 *
	 * @param key variable key
	 * @param value new value or null if the variable is cleared
	 */
	public void put(String key, @Nullable Object value) {
		if (value == null) {
			l1Cache.invalidate(key);
		} else {
			l1Cache.put(key, value);
		}
	}

	/**
	 * Triggers the compaction mechanism.
	 */
	private void triggerCompaction() {
		if (!lifecycleLock.writeLock().tryLock()) {
			// write lock means they either update the compacting flag or already do the compaction,
			// we can safely return
			return;
		}
		try {
			if (isCompacting) return;
			isCompacting = true;
			wastedBytes.set(0);
			Thread.ofVirtual().start(this::compact);
		} finally {
			lifecycleLock.writeLock().unlock();
		}
	}

	/**
	 * Reclaims space by copying live data to a new manager and discarding the old one.
	 */
	private void compact() {
		OffHeapSlabManager newManager = new OffHeapSlabManager(); // survivor space
		Map<String, SlabPointer> newL2Index = new ConcurrentHashMap<>(); // updated L2

		// rewrite all useful data stored before the compaction
		l2Index.entrySet().stream().parallel().forEach(entry -> {
			String key = entry.getKey();
			SlabPointer oldPtr = entry.getValue();
			if (pendingUpdates.containsKey(entry.getKey())) return; // will be copied with current value at phase 2
			MemoryBuffer data = slabManager.read(oldPtr);
			if (data == null) return;
			SlabPointer newPtr = newManager.write(data);
			newL2Index.put(key, newPtr);
		});

		// write all data updates after the compaction (during the copy of useful data - here we lock the world)
		AtomicLong wasted = new AtomicLong(0);
		lifecycleLock.writeLock().lock();
		try {
			pendingUpdates.entrySet().stream().parallel().forEach(update -> {
				String key = update.getKey();
				PendingAction action = update.getValue();
				SlabPointer oldPtr = newL2Index.get(key);
				if (oldPtr != null) wasted.addAndGet(oldPtr.length());
				if (action.isDeletion) {
					newL2Index.remove(key);
				} else {
					MemoryBuffer serialized = fory.serialize(action.value);
					SlabPointer newPtr = newManager.write(serialized);
					newL2Index.put(key, newPtr);
				}
			});
			try {
				slabManager.close();
			} catch (Exception exception) {
				Skript.error("Failed to close old slabs: " + exception.getMessage());
			}
			slabManager = newManager;
			l2Index = newL2Index;
			wastedBytes.set(wasted.longValue());
			pendingUpdates.clear();
		} finally {
			isCompacting = false;
			lifecycleLock.writeLock().unlock();
		}
	}

	public int size() {
		return l2Index.size();
	}

	@Override
	public void close() {
		l1Cache.invalidateAll();
		lifecycleLock.writeLock().lock();
		try {
			slabManager.close();
		} finally {
			lifecycleLock.writeLock().unlock();
		}
	}

	private static ByteBuffer convert(MemoryBuffer memoryBuffer) {
		if (memoryBuffer.isOffHeap()) return memoryBuffer.getOffHeapBuffer();
		return ByteBuffer.wrap(memoryBuffer.getArray());
	}

}
