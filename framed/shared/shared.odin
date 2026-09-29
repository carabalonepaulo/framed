package framed_shared

import cb "../circular_buffer"
import "core:math/bits"

MAX_USER_ARGUMENTS :: #config(FRAMED_MAX_USER_ARGUMENTS, 4)
DEFAULT_PREFIX_LEN :: 2

Endian :: enum u8 {
	Little,
	Big,
}

try_read :: proc(
	expecting: ^int,
	src: ^cb.Circular_Buffer,
	dst: []u8,
	prefix_len: int,
	endian: Endian,
) -> (
	buf: []u8,
	overflow: bool,
	ok: bool,
) {
	if expecting^ == -1 {
		expecting^ = try_read_prefix(src, prefix_len, endian) or_return
	}

	if expecting^ > len(dst) {
		expecting^ = -1
		cb.clear(src)
		return nil, true, false
	}

	dst := dst[:expecting^]
	cb.read(src, dst) or_return
	expecting^ = -1
	return dst, false, true
}

try_write :: proc(
	send_buf: ^cb.Circular_Buffer,
	buf: []u8,
	prefix_len: int,
	endian: Endian,
) -> (
	ok: bool,
) {
	buf_len := len(buf)
	if buf_len == 0 || send_buf.wa < prefix_len + buf_len || u64(buf_len) > get_prefix_max(prefix_len) do return false
	try_write_prefix(send_buf, buf_len, prefix_len, endian)
	return cb.write(send_buf, buf)
}

@(private = "file")
try_read_prefix :: proc(
	src: ^cb.Circular_Buffer,
	prefix_len: int,
	endian: Endian,
) -> (
	expected: int,
	ok: bool,
) {
	expected = -1

	buf: [8]u8
	cb.read(src, buf[:prefix_len]) or_return

	value: u64
	switch endian {
	case .Little:
		for i in 0 ..< prefix_len do value |= u64(buf[i]) << (8 * uint(i))
	case .Big:
		for i in 0 ..< prefix_len do value = (value << 8) | u64(buf[i])
	}

	if value > bits.INT_MAX do return expected, false
	return int(value), true
}

@(private = "file")
try_write_prefix :: proc(
	dst: ^cb.Circular_Buffer,
	packet_len: int,
	prefix_len: int,
	endian: Endian,
) -> (
	ok: bool,
) {
	value := u64(packet_len)
	buf: [8]u8

	switch endian {
	case .Little:
		for i in 0 ..< prefix_len do buf[i] = u8(value >> (8 * uint(i)))
	case .Big:
		for i in 0 ..< prefix_len do buf[prefix_len - 1 - i] = u8(value >> (8 * uint(i)))
	}

	return cb.write(dst, buf[:prefix_len])
}

get_prefix_max :: #force_inline proc(prefix_len: int) -> u64 {
	return ~u64(0) >> u64(64 - prefix_len * 8)
}

