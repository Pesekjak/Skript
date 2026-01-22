package org.skriptlang.skript.variables;

import java.io.Serializable;

/**
 * Points to a specific location in the off-heap slabs.
 *
 * @param slabId the index of the memory slab
 * @param offset the byte offset where the data starts
 * @param length the size of the data in bytes
 */
public record SlabPointer(int slabId, long offset, int length) implements Serializable {
}
