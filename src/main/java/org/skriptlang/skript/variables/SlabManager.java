package org.skriptlang.skript.variables;

import org.apache.fory.memory.MemoryBuffer;

public interface SlabManager extends AutoCloseable {

	SlabPointer write(MemoryBuffer data);

	MemoryBuffer read(SlabPointer ptr);

}
