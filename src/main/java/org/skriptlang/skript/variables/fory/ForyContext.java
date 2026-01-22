package org.skriptlang.skript.variables.fory;

import ch.njol.skript.classes.ClassInfo;
import com.google.common.base.Preconditions;
import org.apache.fory.Fory;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.ForyBuilder;
import org.apache.fory.config.UnknownEnumValueStrategy;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.serializer.Serializer;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

/**
 * Holder of {@link Fory} instance with helper methods to interact with Skript API.
 *
 * @param fory Fory instance
 */
public record ForyContext(Fory fory) {

	/**
	 * @return new default {@link ForyContext}.
	 */
	public static ForyContext create() {
		return of(null);
	}

	/**
	 * Creates new default {@link ForyContext} and applies the provided
	 * consumer before the Fory instance is built.
	 *
	 * @param builderConsumer builder consumer
	 * @return new {@link ForyContext}.
	 */
	public static ForyContext of(@Nullable Consumer<ForyBuilder> builderConsumer) {
		ForyBuilder builder = Fory.builder()
			.withCompatibleMode(CompatibleMode.COMPATIBLE)
			.withUnknownEnumValueStrategy(UnknownEnumValueStrategy.RETURN_NULL)
			.withJdkClassSerializableCheck(true)
			.requireClassRegistration(true);

		if (builderConsumer != null)
			builderConsumer.accept(builder);

		return new ForyContext(builder.build());
	}

	public ForyContext {
		Preconditions.checkNotNull(fory, "Fory instance can not be null");
	}

	/**
	 * Registers new class info to the Fory instance.
	 *
	 * @param classInfo class info to register
	 * @param <T> class info type
	 */
	public <T> void register(ClassInfo<T> classInfo) {
		register(classInfo, (Serializer<T>) null);
	}

	/**
	 * Registers new class info to the Fory instance.
	 *
	 * @param classInfo class info to register
	 * @param skriptSerializer skript serializer used by the class info
	 * @param <T> class info type
	 */
	public <T> void register(ClassInfo<T> classInfo, ch.njol.skript.classes.Serializer<T> skriptSerializer) {
		register(classInfo, new WrappedSerializer<>(fory, classInfo.getC(), skriptSerializer));
	}

	/**
	 * Registers new class info to the Fory instance.
	 *
	 * @param classInfo class info to register
	 * @param serializer serializer to register together with the class info
	 * @param <T> class info type
	 */
	public <T> void register(ClassInfo<T> classInfo, @Nullable Serializer<T> serializer) {
		Preconditions.checkNotNull(classInfo, "ClassInfo to register can not be null");
		Class<T> type = classInfo.getC();
		fory.register(type, classInfo.getCodeName());
		if (serializer != null) fory.registerSerializer(type, serializer);
	}

	/**
	 * @param classInfo class info
	 * @return whether the given class info has a registered serializer
	 */
	public boolean hasSerializer(ClassInfo<?> classInfo) {
		return hasSerializer(classInfo.getCodeName());
	}

	/**
	 * @param codeName code name
	 * @return whether the given code name has a registered serializer
	 */
	public boolean hasSerializer(String codeName) {
		return fory.getClassResolver().isRegisteredByName(codeName);
	}

	/**
	 * @param type type
	 * @return whether the given type has a registered serializer
	 */
	public boolean hasSerializer(Class<?> type) {
		return fory.getClassResolver().isRegisteredById(type);
	}

	/**
	 * Serializes given value and returns heap memory buffer.
	 *
	 * @param value value to serialize
	 * @return heap memory buffer
	 */
	public MemoryBuffer serialize(Object value) {
		MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(64);
		return fory.serialize(buffer, value);
	}

	/**
	 * Deserializes object from the memory buffer.
	 *
	 * @param buffer buffer
	 * @return deserialized object
	 */
	public Object deserialize(MemoryBuffer buffer) {
		return fory.deserialize(buffer);
	}

}
