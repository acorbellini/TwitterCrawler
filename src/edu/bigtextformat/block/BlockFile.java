package edu.bigtextformat.block;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.bigtextformat.header.Header;
import edu.bigtextformat.raw.RawFile;
import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.compression.CompressionType;
import edu.jlime.util.compression.Compressor;

public class BlockFile implements Closeable, Iterable<Block> {
	public static BlockFile create(String path, BlockFileOptions opts,
			Map<String, byte[]> headerData) throws Exception {
		RawFile file = new RawFile(path, opts.trunc, opts.readOnly,
				opts.appendOnly, opts.sync);
		BlockFile ret = new BlockFile(file);
		ret.currentPos = file.length();
		ret.enableCache = opts.enableCache;
		if (file.length() == 0l) {
			ret.reserve(8);
			file.write(0, DataTypeUtils.longToByteArray(opts.magic));
		}
		if (file.length() == 8l || !opts.appendOnly) {
			Map<String, byte[]> map = new HashMap<>();

			if (headerData != null)
				map.putAll(headerData);

			ret.header = new Header();
			if (opts.comp != null) {
				map.put("comp", new byte[] { opts.comp.getType().getId() });
				ret.comp = opts.comp;
				ret.setCompressed(opts.comp);
			} else
				map.put("comp", new byte[] { -1 });
			ret.header = new Header(map);
			Block b = ret.newBlock(ret.header.toByteArray(), false);
			ret.header.setSize(b.size());

		} else {
			ret.header = new Header();
		}

		return ret;
	}

	public static BlockFile open(String path, BlockFileOptions opts)
			throws Exception {
		RawFile file = new RawFile(path, false, opts.readOnly, false, false);

		BlockFile ret = new BlockFile(file);

		ret.currentPos = file.length();
		if (file.length() > 0l) {
			// exists
			ret.magic = file.readLong(0);
			if (ret.magic != opts.magic)
				throw new Exception("Wrong File Type expected "
						+ new String(DataTypeUtils.longToByteArray(opts.magic))
						+ " and got "
						+ new String(DataTypeUtils.longToByteArray(ret.magic))
						+ " for file " + path);
		} else {
			file.close();
			throw new Exception("Corrupted File: File " + file.getFile()
					+ " length is 0");
		}
		Block headerBlock = ret.getBlock(8l);
		// headerBlock.setFixed(true);
		// headerBlock.setMemoryMapped(true);
		ret.header = Header.open(headerBlock);

		byte compression = ret.header.get("comp")[0];
		if (compression != -1)
			ret.comp = CompressionType.getByID(compression);
		return ret;
	}

	// boolean reuseDeleted;

	// TreeMap<Integer, List<Long>> deleted = new TreeMap<>();

	public static BlockFile open(String path, long magicCheck) throws Exception {
		return open(path, new BlockFileOptions().setMagic(magicCheck)
				.setReadOnly(true));
	}

	private static final long MAX_CACHE_SIZE = 0;

	private Cache<BlockID, Block> blocks = CacheBuilder.newBuilder()
			.softValues().maximumSize(MAX_CACHE_SIZE).build();

	private List<BlockPosChangeListener> blockPosChangeListeners = new ArrayList<>();

	RawFile file;

	private Header header;

	long magic;

	private Compressor comp;

	// private static WeakHashMap<Block, Boolean> current = new WeakHashMap<>();
	//
	// private static WeakHashMap<Long, Block> blocks = new WeakHashMap<>();

	UUID id = UUID.randomUUID();

	long currentPos = 0;

	private boolean enableCache;

	private BlockFile(RawFile file) {
		this.file = file;
	}

	public void addPosListener(BlockPosChangeListener l) {
		blockPosChangeListeners.add(l);
	}

	public long appendBlock(BlockFile orig, long pos, long long1)
			throws IOException {
		long s = reserve(long1);
		this.file.copy(orig.getRawFile(), pos, long1, s);
		return s;
	}

	public void close() throws IOException {
		file.close();
	}

	public void delete() throws IOException {
		file.delete();
	}

	public void flush() throws IOException {
		file.sync();
	}

	public Block getBlock(final long pos) throws Exception {
		try {
			if (!enableCache)
				return doGetBlock(pos);
			else
				return blocks.get(BlockID.create(id, pos),
						new Callable<Block>() {
							@Override
							public Block call() throws Exception {
								return doGetBlock(pos);
							}

						});

		} catch (MissingBlockException e) {
			throw e;
		} catch (Exception e) {
			throw new CorruptedFileException(this, pos, e);
		}
	}

	private Block doGetBlock(final long pos) throws Exception {
		if (pos > 0 && currentPos > pos) {
			RawFile raw = getRawFile();
			int blockSize = raw.readInt(pos + 8);
			byte[] data = new byte[blockSize];
			raw.read(pos, data);
			return new Block(pos, data.length).fromByteArray(data);
		}
		throw new MissingBlockException(pos, file);
	}

	protected Block getFirstBlock() throws Exception {
		return getBlock(getFirstBlockPos());
	}

	public long getFirstBlockPos() {
		return 8l + header.getSize();
	}

	public Header getHeader() {
		return header;
	}

	public Block getLastBlock() throws Exception {
		return getBlock(getLastBlockPosition());
	}

	public long getLastBlockPosition() throws Exception {
		int size = file.readInt(file.length() - 8 - 4);
		long pos = file.length() - size;
		return pos;
	}

	public RawFile getRawFile() {
		return file;
	}

	public boolean isEmpty() throws Exception {
		return length() == getFirstBlockPos();
	}

	@Override
	public Iterator<Block> iterator() {
		try {
			return new BlockFileIterator(this);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public long length() throws Exception {
		return file.length();
	}

	public Block newCompressedBlock(byte[] bytes) throws Exception {
		return newBlock(bytes, true);
	}

	public Block newBlock(byte[] bytes, boolean compressed) throws Exception {

		Block b = new Block(bytes, comp);

		byte[] asBytes = b.toByteArray();

		long pos = reserve(asBytes.length);

		b.written(pos, asBytes.length);

		file.write(pos, asBytes);

		if (pos <= 0)
			throw new Exception("You shouldn't be writing on pos 0");

		if (enableCache)
			synchronized (blocks) {
				blocks.put(BlockID.create(id, pos), b);
			}

		return b;
	}

	void notifyPosChanged(Block b, long oldPos) throws Exception {
		for (BlockPosChangeListener block : blockPosChangeListeners) {
			block.changedPosition(b, oldPos);
		}
	}

	public long reserve(long long1) {
		synchronized (blocks) {
			long res = currentPos;
			currentPos += long1;
			return res;
		}
	}

	public void setCompressed(Compressor comp) {
		this.comp = comp;
	}

	public long size() throws Exception {
		return file.length();
	}

	@Override
	public String toString() {
		return this.file.getFile().getPath();
	}
}
