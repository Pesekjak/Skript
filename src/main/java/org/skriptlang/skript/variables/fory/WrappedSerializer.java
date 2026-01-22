package org.skriptlang.skript.variables.fory;

import ch.njol.yggdrasil.Fields;
import com.google.common.base.Preconditions;
import org.apache.fory.Fory;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.serializer.Serializer;

import java.io.NotSerializableException;
import java.io.StreamCorruptedException;

/**
 * Fory serializer implementation that wraps around existing Yggdrasil based Skript serializer.
 * <p>
 * This should only be used for backwards compatibility.
 *
 * @param <T> type
 */
public class WrappedSerializer<T> extends Serializer<T> {

	private final ch.njol.skript.classes.Serializer<T> skriptSerializer;

	public WrappedSerializer(Fory fory, Class<T> type, ch.njol.skript.classes.Serializer<T> skriptSerializer) {
		super(fory, type);
		this.skriptSerializer = Preconditions.checkNotNull(skriptSerializer,
			"Wrapped skript serializer can not be null");
	}

	@Override
	public void write(MemoryBuffer buffer, T value) {
		try {
			Fields fields = skriptSerializer.serialize(value);
			buffer.writeInt32(fields.size());
			for (Fields.FieldContext field : fields) {
				fory.writeString(buffer, field.getID());
				buffer.writeBoolean(field.isPrimitive());
				fory.writeRef(buffer, field.isPrimitive() ? field.getPrimitive() : field.getObject());
			}
		} catch (NotSerializableException | StreamCorruptedException exception) {
			throw new RuntimeException(exception);
		}
	}

	@Override
	public T read(MemoryBuffer buffer) {
		if (!skriptSerializer.canBeInstantiated(type))
			throw new UnsupportedOperationException("Type " + type.getName() + " can not be instantiated.");
		int size = buffer.readInt32();
		Fields fields = new Fields();
		for (int i = 0; i < size; i++) {
			String name = fory.readString(buffer);
			boolean primitive = buffer.readBoolean();
			Object ref = fory.readRef(buffer);
			if (primitive) fields.putPrimitive(name, ref);
			else fields.putObject(name, ref);
		}
		try {
			T instance = skriptSerializer.newInstance(type);
			skriptSerializer.deserialize(instance, fields);
			return instance;
		} catch (NotSerializableException | StreamCorruptedException exception) {
			throw new RuntimeException(exception);
		}
	}

}
