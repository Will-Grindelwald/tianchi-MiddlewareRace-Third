package io.openmessaging.demo;

public class Utils {

	// char
	public static char getChar(byte[] b, int start) {
		return (char) ((b[start] & 0xFF) << 8 | b[start + 1] & 0xFF);
	}

	// int
	public static int getInt(byte[] b, int start) {
		return (b[start] & 0xFF) << 24 | (b[start + 1] & 0xFF) << 16 | (b[start + 2] & 0xFF) << 8 | b[start + 3] & 0xFF;
	}

	public static byte[] intToByteArray(int i) {
		return new byte[] { (byte) ((i >> 24) & 0xFF), (byte) ((i >> 16) & 0xFF), (byte) ((i >> 8) & 0xFF),
				(byte) (i & 0xFF) };
	}

	public static boolean intToByteArray(int i, byte[] b, int start) {
		if (!checkBoard(b, start, 4))
			return false;
		b[start] = (byte) ((i >> 24) & 0xFF);
		b[start + 1] = (byte) ((i >> 16) & 0xFF);
		b[start + 2] = (byte) ((i >> 8) & 0xFF);
		b[start + 3] = (byte) (i & 0xFF);
		return true;
	}

	// long
	public static long getLong(byte[] b, int start) {
		return (b[start] & 0xFFL) << 56 | (b[start + 1] & 0xFFL) << 48 | (b[start + 2] & 0xFFL) << 40
				| (b[start + 3] & 0xFFL) << 32 | (b[start + 4] & 0xFFL) << 24 | (b[start + 5] & 0xFFL) << 16
				| (b[start + 6] & 0xFFL) << 8 | b[start + 7] & 0xFFL;
	}

	public static byte[] longToByteArray(long l) {
		return new byte[] { (byte) ((l >> 56) & 0xFF), (byte) ((l >> 48) & 0xFF), (byte) ((l >> 40) & 0xFF),
				(byte) ((l >> 32) & 0xFF), (byte) ((l >> 24) & 0xFF), (byte) ((l >> 16) & 0xFF),
				(byte) ((l >> 8) & 0xFF), (byte) (l & 0xFF) };
	}

	public static boolean longToByteArray(long l, byte[] b, int start) {
		if (!checkBoard(b, start, 8))
			return false;
		b[start] = (byte) ((l >> 56) & 0xFF);
		b[start + 1] = (byte) ((l >> 48) & 0xFF);
		b[start + 2] = (byte) ((l >> 40) & 0xFF);
		b[start + 3] = (byte) ((l >> 32) & 0xFF);
		b[start + 4] = (byte) ((l >> 24) & 0xFF);
		b[start + 5] = (byte) ((l >> 16) & 0xFF);
		b[start + 6] = (byte) ((l >> 8) & 0xFF);
		b[start + 7] = (byte) (l & 0xFF);
		return true;
	}

	// double
	public static double getDouble(byte[] b, int start) {
		return Double.longBitsToDouble(getLong(b, start));
	}

	public static byte[] doubleToByteArray(double d) {
		return longToByteArray(Double.doubleToRawLongBits(d));
	}

	public static boolean doubleToByteArray(double d, byte[] b, int start) {
		return longToByteArray(Double.doubleToRawLongBits(d), b, start);
	}

	public static boolean checkBoard(byte[] b, int start, int lenth) {
		if (b.length - start < lenth)
			return false;
		return true;
	}
}
