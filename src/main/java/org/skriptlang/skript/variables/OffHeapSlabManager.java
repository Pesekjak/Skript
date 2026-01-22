package org.skriptlang.skript.variables;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import ch.njol.skript.Skript;
import com.google.common.base.Preconditions;
import org.apache.fory.memory.MemoryBuffer;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class OffHeapSlabManager implements SlabManager {

	private static final long SLAB_SIZE = 32 * 1024 * 1024; // 32 MB per slab

	private static final int STRIPE_COUNT = 16;
	private static final int STRIPE_MASK = STRIPE_COUNT - 1;

	private final ConcurrentHashMap<Integer, MemorySegment> directory = new ConcurrentHashMap<>();

	private final Stripe[] stripes;

	private final Arena arena;
	private final AtomicInteger nextSlabId = new AtomicInteger(0);

	private volatile boolean isClosed = false;
	private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

	public OffHeapSlabManager() {
		arena = Arena.ofShared();
		stripes = new Stripe[STRIPE_COUNT];
		for (int i = 0; i < STRIPE_COUNT; i++) {
			stripes[i] = new Stripe();
		}
	}

	@Override
	public SlabPointer write(MemoryBuffer data) {
		int size = data.size();
		Preconditions.checkState(size <= SLAB_SIZE, "Variable too large for slab");
		int hash = System.identityHashCode(Thread.currentThread());
		int stripeIndex = (hash ^ (hash >>> 16)) & STRIPE_MASK;
		lifecycleLock.readLock().lock();
		try {
			if (isClosed) throw new IllegalStateException("Manager is closed");
			return stripes[stripeIndex].write(data, size);
		} finally {
			lifecycleLock.readLock().unlock();
		}
	}

	@Override
	public MemoryBuffer read(SlabPointer ptr) {
		if (isClosed) return null;
		lifecycleLock.readLock().lock();
		try {
			if (isClosed) return null;
			MemorySegment slab = directory.get(ptr.slabId());
			if (slab == null) return null;
			MemorySegment slice = slab.asSlice(ptr.offset(), ptr.length());
			return MemoryBuffer.fromByteBuffer(slice.asByteBuffer());
		} catch (IndexOutOfBoundsException _) {
			return null;
		} finally {
			lifecycleLock.readLock().unlock();
		}
	}

	private SlabAllocation allocateSlab() {
		try {
			MemorySegment segment = arena.allocate(SLAB_SIZE);
			int id = nextSlabId.getAndIncrement();
			directory.put(id, segment);
			return new SlabAllocation(id, segment);
		} catch (OutOfMemoryError exception) {
			Skript.error("Out of native memory for Skript variables");
			throw exception;
		}
	}

	@Override
	public void close() {
		lifecycleLock.writeLock().lock(); // assure all stripes locks are free as well
		try {
			if (isClosed) return;
			isClosed = true;
			if (arena.scope().isAlive()) arena.close();
			directory.clear();
		} finally {
			lifecycleLock.writeLock().unlock();
		}
	}

	private record SlabAllocation(int id, MemorySegment segment) {}

	private class Stripe {

		private final ReentrantLock lock = new ReentrantLock();
		private MemorySegment currentSlab;
		private int currentId = -1;
		private long writeOffset = 0;

		private void assureAvailable(int size) {
			if (currentSlab != null && writeOffset + size <= SLAB_SIZE)
				return;
			SlabAllocation alloc = allocateSlab();
			currentSlab = alloc.segment;
			currentId = alloc.id;
			writeOffset = 0;
		}

		public SlabPointer write(MemoryBuffer data, int size) {
			lock.lock();
			try {
				// we are writing to 'currentSlab' which is only modified by this stripe
				// readers might be reading from it concurrently but that is safe because
				// they read from it only after we return the pointer to the data
				assureAvailable(size);
				if (data.isOffHeap()) {
					MemorySegment srcSegment = MemorySegment.ofBuffer(data.getOffHeapBuffer());
					MemorySegment.copy(srcSegment, 0, currentSlab, writeOffset, size);
				} else {
					MemorySegment.copy(data.getArray(), 0, currentSlab, ValueLayout.JAVA_BYTE, writeOffset, size);
				}
				SlabPointer ptr = new SlabPointer(currentId, writeOffset, size);
				writeOffset += size;
				return ptr;

			} finally {
				lock.unlock();
			}
		}
	}

}
