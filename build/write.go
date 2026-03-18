package build

import "net"

/* ======================================================
 * MySQL Packet Helpers
 * ====================================================== */

func WritePacket(conn net.Conn, seq *uint8, payload []byte) error {
	l := len(payload)
	header := []byte{byte(l), byte(l >> 8), byte(l >> 16), *seq}
	*seq++
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func WriteEOF(conn net.Conn, seq *uint8) {
	_ = WritePacket(conn, seq, []byte{0xFE, 0x00, 0x00, 0x00, 0x00})
}
