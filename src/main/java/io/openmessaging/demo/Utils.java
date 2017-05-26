package io.openmessaging.demo;

public class Utils {

	public static int getInt(byte[] b, int start) {
		return (b[start] & 0xFF) << 24 | (b[start + 1] & 0xFF) << 16 | (b[start + 2] & 0xFF) << 8 | b[start + 3] & 0xFF;
	}

	public static long getLong(byte[] b, int start) {
		return (b[start] & 0xFFL) << 56 | (b[start + 1] & 0xFFL) << 48 | (b[start + 2] & 0xFFL) << 40
				| (b[start + 3] & 0xFFL) << 32 | (b[start + 4] & 0xFFL) << 24 | (b[start + 5] & 0xFFL) << 16
				| (b[start + 6] & 0xFFL) << 8 | b[start + 7] & 0xFFL;
	}
}
