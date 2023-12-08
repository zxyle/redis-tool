package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4"
	"io"
	"os"
	"strconv"
)

// https://redis.io/docs/data-types/
const (
	String = iota
	List
	Set
	Hash
	ZSet
)

type RedisKey struct {
	db      uint   // 数据库编号
	key     string // redis key
	expire  uint64 // 过期时间
	bytes   uint32 // key占用字节数
	kind    uint8  // 类型 0: string 1: list 2: set 3: zset 4: hash
	element uint32 // 元素数量
}

const (
	IDLE         = 248
	SelectDb     = 254
	Aux          = 250
	ResizeDd     = 251
	ExpiretimeMs = 252
	EXPIRETIME   = 253
	EOF          = 255 // 0xFF
)

const (
	RedisRdb6bitlen  = 0
	RedisRdb14bitlen = 1
	RedisRdb32bitlen = 0x80
	RedisRdb64bitlen = 0x81
	RedisRdbEncval   = 3
)

const (
	RedisRdbTypeString          = 0
	RedisRdbTypeList            = 1
	RedisRdbTypeSet             = 2
	RedisRdbTypeZset            = 3
	RedisRdbTypeHash            = 4
	RedisRdbTypeZset2           = 5 // ZSET version 2 with doubles stored in binary.
	RedisRdbTypeModule          = 6
	RedisRdbTypeModule2         = 7
	RedisRdbTypeHashZipmap      = 9
	RedisRdbTypeListZiplist     = 10
	RedisRdbTypeSetIntset       = 11
	RedisRdbTypeZsetZiplist     = 12
	RedisRdbTypeHashZiplist     = 13
	RedisRdbTypeListQuicklist   = 14
	RedisRdbTypeStreamListpacks = 15
	// hyperloglog
)

const (
	REDIS_RDB_ENC_INT8  = 0
	REDIS_RDB_ENC_INT16 = 1
	REDIS_RDB_ENC_INT32 = 2
	REDIS_RDB_ENC_LZF   = 3
)

// 读取有符号
func readSignedChar(reader *bufio.Reader) int8 {
	var value int8
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func readSignedShort(reader *bufio.Reader) int16 {
	var value int16
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func readUnsignedChar(reader *bufio.Reader) uint8 {
	b, _ := reader.ReadByte()
	return b
}

func readUnsignedShort(reader *bufio.Reader) uint16 {
	b1, _ := reader.ReadByte()
	b2, _ := reader.ReadByte()
	return uint16(b1)<<8 + uint16(b2)
}

func readSignedInt(reader *bufio.Reader) int32 {
	var value int32
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func readKey(reader *bufio.Reader) (uint8, string) {
	keyLength := readUnsignedChar(reader)
	bytes := make([]byte, keyLength)
	reader.Read(bytes)
	return keyLength, string(bytes)
}

func readUnsignedIntBe(reader *bufio.Reader) uint32 {
	var value uint32
	binary.Read(reader, binary.BigEndian, &value)
	return value
}

func readUnsignedLongBe(reader *bufio.Reader) uint64 {
	var value uint64
	binary.Read(reader, binary.BigEndian, &value)
	return value
}

func readUnsignedLong(reader *bufio.Reader) uint64 {
	var value uint64
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func readLengthWithEncoding(reader *bufio.Reader) (uint32, bool) {
	var length uint32
	isEncoded := false
	bytes := make([]byte, 0)
	bytes = append(bytes, readUnsignedChar(reader))
	encType := bytes[0] & 0xC0 >> 6
	switch encType {
	case RedisRdbEncval:
		isEncoded = true
		b := bytes[0] & 0x3F
		length = uint32(b)
	case RedisRdb6bitlen:
		b := bytes[0] & 0x3F
		length = uint32(b)
	case RedisRdb14bitlen:
		bytes = append(bytes, readUnsignedChar(reader))
		b := (uint32(bytes[0]&0x3F) << 8) | uint32(bytes[1])
		length = b
	}
	if bytes[0] == RedisRdb32bitlen {
		length = readUnsignedIntBe(reader)
	}
	if bytes[0] == RedisRdb64bitlen {
		length = uint32(uint8(readUnsignedLongBe(reader)))
	}

	return length, isEncoded
}

func readLength(reader *bufio.Reader) uint32 {
	encoding, _ := readLengthWithEncoding(reader)
	return encoding
}

func readString(reader *bufio.Reader) string {
	length, isEncoded := readLengthWithEncoding(reader)
	if isEncoded {
		if length == REDIS_RDB_ENC_INT8 {
			char := readSignedChar(reader)
			return fmt.Sprintf("%d", char)
		} else if length == REDIS_RDB_ENC_INT16 {
			short := readSignedShort(reader)
			return strconv.Itoa(int(short))
		} else if length == REDIS_RDB_ENC_INT32 {
			integer := readSignedInt(reader)
			return strconv.FormatInt(int64(integer), 10)
		} else if length == REDIS_RDB_ENC_LZF {
			//readByte, _ := reader.ReadByte()
			//fmt.Println("读取到有符号 长度", readByte)
			clen := readLength(reader)
			l := readLength(reader)
			//fmt.Println("chen", clen)
			fmt.Println("l", l)
			bytes := make([]byte, clen)
			reader.Read(bytes)
			// TODO 使用lzf解压
			//s, _ := decompress(bytes, l)
			//fmt.Println("LZF解压: ", s)
			return string(bytes[:10])
		} else {
			panic("未知类型")
		}
	} else {
		byes := make([]byte, length)
		reader.Read(byes)
		return string(byes)
	}
	return ""
}

func decompress(compressedData []byte, l uint32) (string, error) {
	output := make([]byte, l) // 10 is just a multiplier, you may adjust it based on your data characteristics

	n, err := lz4.UncompressBlock(compressedData, output)
	if err != nil {
		return "", err
	}

	return string(output[:n]), nil
}

func readMillisecondsTime(reader *bufio.Reader) {
	readUnsignedLong(reader)
	//milliseconds := readUnsignedLong(reader)
	//fmt.Println("过期时间", milliseconds)
}

func readHashFromZiplist(reader *bufio.Reader) {
	readString(reader)
	//fmt.Println("rawString", rawString)
	// TODO 等待实现
}

func readIntSet(reader *bufio.Reader) []int64 {
	rawString := readString(reader)
	bytes1 := []byte(rawString)
	encoding := binary.LittleEndian.Uint32(bytes1[:4])
	numEntries := binary.LittleEndian.Uint32(bytes1[4:8])

	int64s := make([]int64, 0)
	start := 8
	for i := 0; i < int(numEntries); i++ {
		if encoding == 8 {
			entry := int64(binary.BigEndian.Uint32(bytes1[8:]))
			int64s = append(int64s, entry)
		} else if encoding == 4 {
			entry := int64(binary.LittleEndian.Uint32(bytes1[start : start+4]))
			int64s = append(int64s, entry)
			start += 4
		} else if encoding == 2 {
			entry := int64(binary.LittleEndian.Uint16(bytes1[start : start+2]))
			int64s = append(int64s, entry)
			start += 2
		}
	}

	return int64s
}

func readBinaryDouble(reader *bufio.Reader) float64 {
	var value float64
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func read_float(reader *bufio.Reader) {
	char := readUnsignedChar(reader)
	if char == 253 {

	} else if char == 254 {
	} else if char == 255 {
	} else {
	}
}

func readZsetFromZiplist(reader *bufio.Reader) {
	readString(reader)
	//fmt.Println("rawString", rawString)

	// todo 待解析
}

func readUnsignedInt(reader *bufio.Reader) uint32 {
	var value uint32
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func readSignedLong(reader *bufio.Reader) int64 {
	var value int64
	binary.Read(reader, binary.BigEndian, &value)
	return value
}

func read_ziplist_entry(bytes []byte) string {
	prev_length := bytes[0]
	if prev_length == 254 {
		//prev_length := readUnsignedInt(reader)
	}
	entry_header := bytes[1]
	if entry_header>>6 == 0 {
		length := entry_header & 0x3F
		value := bytes[2 : 2+length]
		return string(value)
	} else if entry_header>>6 == 1 {
		length := entry_header&0x3F | bytes[2]
		value := bytes[3 : 3+length]
		fmt.Println(value)
	} else if entry_header>>6 == 2 {
		//length := readUnsignedIntBe(reader)
		//value := make([]byte, length)
		//fmt.Println(value)
	} else if entry_header>>4 == 12 {
		//length := readSignedShort(reader)
		//value := make([]byte, length)
		//fmt.Println(value)
	} else if entry_header>>4 == 13 {
		//length := readSignedInt(reader)
		//value := make([]byte, length)
		//fmt.Println(value)
	} else if entry_header>>4 == 14 {
		//length := readSignedLong(reader)
		//value := make([]byte, length)
		//fmt.Println(value)
	} else if entry_header == 240 {

	}
	return ""

}

func readListFromQuicklist(reader *bufio.Reader) {
	count := readLength(reader)
	totalSize := 0
	//fmt.Println("count", count)
	for i := 0; i < int(count); i++ {
		rawString := readString(reader)
		totalSize += len(rawString)
		bytes1 := []byte(rawString)
		//zlbytes := binary.LittleEndian.Uint32(bytes1[:4])
		//tail_offset := binary.LittleEndian.Uint32(bytes1[4:8])
		//fmt.Println("zlbytes", zlbytes, "tail_offset", tail_offset)
		numEntries := binary.LittleEndian.Uint16(bytes1[8:10])
		elements := make([]string, 0)
		for i := 0; i < int(numEntries); i++ {
			entry := read_ziplist_entry(bytes1[10:])
			elements = append(elements, entry)
		}
		fmt.Println(elements)
	}
}

func main() {
	fmt.Println("开始解析")

	// 读取文件
	//fileName := "/Users/xiangzheng/dump.rdb"
	fileName := "/Users/xiangzheng/Downloads/hins22112154_data_20231207012206.rdb"
	// 打开文件
	file, _ := os.Open(fileName)
	reader := bufio.NewReaderSize(file, 50*1024*1024)

	defer file.Close()

	// 解析文件
	magicString := make([]byte, 9)
	file.Read(magicString)
	fmt.Println(string(magicString))

	keys := make([]RedisKey, 0)
	var db uint = 0

	for {
		b, err := reader.ReadByte()
		if b == Aux {
			b, _ := reader.ReadByte()
			// fmt.Println("key长度", b)
			bytes := make([]byte, b)
			reader.Read(bytes)
			fmt.Print(string(bytes))
			b, _ = reader.ReadByte()
			isEncoded := false
			length := 0
			encType := b & 0xC0 >> 6
			switch encType {
			case 3:
				isEncoded = true
				length = int(b & 0x3F)
			case 0:
				length = int(b & 0x3F)
			}

			if isEncoded {
				if length == 0 {
					val := readSignedChar(reader)
					fmt.Println(" ", val)
					continue
				} else if length == 2 {
					signedInt := readSignedInt(reader)
					fmt.Println(" ", signedInt)
					continue
				}
			}
			// fmt.Println("value长度", b)
			bytes = make([]byte, b)
			reader.Read(bytes)
			fmt.Println(" ", string(bytes))
		} else if b == SelectDb {
			dbNumber := readUnsignedChar(reader)
			fmt.Println("SelectDb: ", dbNumber)
			db = uint(dbNumber)
		} else if b == ResizeDd {
			dbSize := readLength(reader)
			expireSize := readLength(reader)
			fmt.Println("dbSize", dbSize, "expireSize", expireSize)
		} else if b == RedisRdbTypeListQuicklist {
			_, key := readKey(reader)

			// list key后面有一个字节数据，不清楚作用
			//count := readUnsignedChar(reader)
			//bytes := readLength(reader)
			//
			//valueBytes := make([]byte, bytes)
			//reader.Read(valueBytes)
			//fmt.Println("List key:", key)

			fmt.Print("List key: ", key, " 元素: ")
			readListFromQuicklist(reader)
			keys = append(keys, RedisKey{db, key, 0, 0, List, 0})
		} else if b == RedisRdbTypeString {
			_, key := readKey(reader)
			value := readString(reader)
			fmt.Println("Str key:", key, "value:", value)
			// 读取但不打印为了测试
			keys = append(keys, RedisKey{db, key, 0, uint32(len(value)), String, 1})
		} else if b == ExpiretimeMs {
			// 过期时间
			readMillisecondsTime(reader)
			//b, _ = reader.ReadByte()
			//readKey(reader)

		} else if b == EXPIRETIME {
			fmt.Println("待解析过期时间")
		} else if b == EOF {
			crc32CheckSum := make([]byte, 8)
			reader.Read(crc32CheckSum)
			break
		} else if b == RedisRdbTypeZsetZiplist {
			_, key := readKey(reader)
			fmt.Println("ZSET(ziplist), key:", key)
			readZsetFromZiplist(reader)
			keys = append(keys, RedisKey{db, key, 0, 0, ZSet, 1})
		} else if b == IDLE {
			// 返回idle的值，猜测可能是剩余秒数
			readLength(reader)
		} else if b == RedisRdbTypeHashZiplist {
			_, key := readKey(reader)
			//fmt.Println("hash_ziplist, key:", key)
			readHashFromZiplist(reader)
			keys = append(keys, RedisKey{db, key, 0, 0, List, 1})
		} else if b == RedisRdbTypeSet {
			_, key := readKey(reader)
			length := readLength(reader)
			fmt.Println("Set key: ", key, "元素数量: ", length)
			// for 循环
			for i := 0; i < int(length); i++ {
				readString(reader)
			}
			keys = append(keys, RedisKey{db, key, 0, 0, Set, length})
		} else if b == RedisRdbTypeSetIntset {
			_, key := readKey(reader)
			members := readIntSet(reader)
			fmt.Println("Set(int), key:", key, ", Value: ", members)
			keys = append(keys, RedisKey{db, key, 0, 0, Set, uint32(len(members))})
		} else if b == RedisRdbTypeZset2 || b == RedisRdbTypeZset {
			lens, key := readKey(reader)
			membersLength := readLength(reader)
			fmt.Println("ZSET, key:", key, "长度:", lens, "元素数量:", membersLength)
			for i := 0; i < int(membersLength); i++ {
				readString(reader) // member
				if b == RedisRdbTypeZset2 {
					readBinaryDouble(reader) // score
					//fmt.Println("member:", member, ", score:", int(score))
				}
				if b == RedisRdbTypeZset {
				}
			}
			keys = append(keys, RedisKey{db, key, 0, 0, ZSet, membersLength})
		} else if b == RedisRdbTypeHash {
			_, key := readKey(reader)
			length := readLength(reader)
			fmt.Println("HASH, key:", key, "长度:", length)
			for i := 0; i < int(length); i++ {
				field := readString(reader)
				value := readString(reader)
				fmt.Println("field:", field, ", value:", value)
			}
			keys = append(keys, RedisKey{db, key, 0, 0, Hash, length})

		} else {
			_, key := readKey(reader)
			fmt.Println("读取到未知类型, key:", key)
			break
		}

		if err == io.EOF {
			fmt.Println("读取完毕！")
			break
		}
	}

	fmt.Println("keys: ", keys)
	fmt.Println("keys数量: ", len(keys))
}
