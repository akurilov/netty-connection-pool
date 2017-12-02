package com.github.akurilov.netty.connection.pool.util;

import com.emc.mongoose.api.model.data.DataInput;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.MappedByteBuffer;

/**
 * Created by andrey on 17.11.17.
 */
public class DummyDataInput
implements DataInput {

	@Override
	public final int getSize() {
		return 0;
	}

	@Override
	public final MappedByteBuffer getLayer(final int layerIndex) {
		return null;
	}

	@Override
	public final void close()
	throws IOException {

	}

	@Override
	public final void writeExternal(final ObjectOutput out)
	throws IOException {

	}

	@Override
	public final void readExternal(final ObjectInput in)
	throws IOException, ClassNotFoundException {

	}
}
