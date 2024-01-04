package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
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
	ModuleAux    = 247
	IDLE         = 248
	FREQ         = 249
	Aux          = 250
	ResizeDd     = 251
	ExpiretimeMs = 252
	EXPIRETIME   = 253
	SelectDb     = 254 // DB number of the following keys.
	EOF          = 255 // End of the RDB file. 0xFF
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
	RedisRdbTypeList            = 1 // 猜测可能是linkedlist
	RedisRdbTypeSet             = 2
	RedisRdbTypeZset            = 3
	RedisRdbTypeHash            = 4 // Hash hashtable
	RedisRdbTypeZset2           = 5 // ZSET version 2 with doubles stored in binary.
	RedisRdbTypeModule          = 6
	RedisRdbTypeModule2         = 7
	RedisRdbTypeHashZipmap      = 9
	RedisRdbTypeListZiplist     = 10
	RedisRdbTypeSetIntset       = 11 // 存储整数的集合
	RedisRdbTypeZsetZiplist     = 12
	RedisRdbTypeHashZiplist     = 13
	RedisRdbTypeListQuicklist   = 14
	RedisRdbTypeStreamListpacks = 15
)

const (
	RedisRdbEncInt8  = 0
	RedisRdbEncInt16 = 1
	RedisRdbEncInt32 = 2
	RedisRdbEncLzf   = 3
)

func readSignedChar(reader *bufio.Reader) int8 {
	var value int8
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func readUnsignedChar(reader *bufio.Reader) uint8 {
	b, _ := reader.ReadByte()
	return b
}

func readSignedShort(reader *bufio.Reader) int16 {
	var value int16
	binary.Read(reader, binary.LittleEndian, &value)
	return value
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

func readKey(reader *bufio.Reader) string {
	// 读取key长度
	keyLength := readUnsignedChar(reader)
	// 读取key
	bytes := make([]byte, keyLength)
	reader.Read(bytes)
	return string(bytes)
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

func readSignedLong(reader *bufio.Reader) int64 {
	var value int64
	binary.Read(reader, binary.BigEndian, &value)
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
		if length == RedisRdbEncInt8 {
			char := readSignedChar(reader)
			return fmt.Sprintf("%d", char)
		} else if length == RedisRdbEncInt16 {
			short := readSignedShort(reader)
			return strconv.Itoa(int(short))
		} else if length == RedisRdbEncInt32 {
			integer := readSignedInt(reader)
			return strconv.FormatInt(int64(integer), 10)
		} else if length == RedisRdbEncLzf {
			//readByte, _ := reader.ReadByte()
			//fmt.Println("读取到有符号 长度", readByte)
			clen := readLength(reader)
			readLength(reader)
			//fmt.Println("chen", clen)
			//fmt.Println("l", l)
			bytes := make([]byte, clen)
			reader.Read(bytes)
			// TODO 使用lzf解压
			return string(bytes)
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

func readMillisecondsTime(reader *bufio.Reader) uint64 {
	ts := readUnsignedLong(reader)
	return ts
}

func readHashFromZiplist(reader *bufio.Reader) uint16 {
	rawString := readString(reader)
	bytes := []byte(rawString)
	_ = binary.LittleEndian.Uint32(bytes[:4])       // zlbytes 4byte
	_ = binary.LittleEndian.Uint32(bytes[4:8])      // zltail  4byte
	zlen := binary.LittleEndian.Uint16(bytes[8:10]) // zle     2byte
	numEntries := zlen / 2
	//fmt.Println("zlbytes", zlbytes, "zltail", zltail, "zlen", zlen, "numEntries", numEntries)

	// TODO 等待实现读取entry， entry的结构为 entry-header，entry-encoding，entry-data
	// entry-header 1字节，entry-encoding 1字节，entry-data 1字节
	//startIndex := 0
	bytes = bytes[10:]
	for i := 0; i < int(numEntries); i++ {
		// | Prev Length | Encoding | Data | Length |
		field, index := readZiplistEntry(bytes)
		bytes = bytes[index:]
		value, index := readZiplistEntry(bytes)
		bytes = bytes[index:]
		fmt.Println("field", field, "value", value)
	}

	// ziplist结构结尾字节固定为 0xFF
	zlend := bytes[len(bytes)-1]
	if zlend != 0xff {
		panic("ziplist结尾不为0xff")
	}
	return numEntries
}

func readIntSet(reader *bufio.Reader) []int64 {
	rawString := readString(reader)
	bytes := []byte(rawString)
	encoding := binary.LittleEndian.Uint32(bytes[:4])
	numEntries := binary.LittleEndian.Uint32(bytes[4:8])

	intArr := make([]int64, 0)
	start := 8
	for i := 0; i < int(numEntries); i++ {
		if encoding == 8 {
			entry := int64(binary.BigEndian.Uint32(bytes[8:]))
			intArr = append(intArr, entry)
		} else if encoding == 4 {
			entry := int64(binary.LittleEndian.Uint32(bytes[start : start+4]))
			intArr = append(intArr, entry)
			start += 4
		} else if encoding == 2 {
			entry := int64(binary.LittleEndian.Uint16(bytes[start : start+2]))
			intArr = append(intArr, entry)
			start += 2
		}
	}

	return intArr
}

func readBinaryDouble(reader *bufio.Reader) float64 {
	var value float64
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

func readFloat(reader *bufio.Reader) {
	char := readUnsignedChar(reader)
	if char == 253 {

	} else if char == 254 {
	} else if char == 255 {
	} else {
	}
}

func readZsetFromZiplist(reader *bufio.Reader) []string {
	rawString := readString(reader)
	bytes := []byte(rawString)
	_ = binary.LittleEndian.Uint32(bytes[:4])             // zlbytes
	_ = binary.LittleEndian.Uint32(bytes[4:8])            // zltail
	numEntries := binary.LittleEndian.Uint16(bytes[8:10]) // zllen
	members := make([]string, 0)
	numEntries = numEntries / 2
	bytes = bytes[10:]
	for i := 0; i < int(numEntries); i++ {
		member, index := readZiplistEntry(bytes)
		bytes = bytes[index:]
		// score, 无用
		score, index := readZiplistEntry(bytes)
		bytes = bytes[index:]
		fmt.Println("member", member, "score", score)
		members = append(members, member)
	}
	return members
}

func readUnsignedInt(reader *bufio.Reader) uint32 {
	var value uint32
	binary.Read(reader, binary.LittleEndian, &value)
	return value
}

// https://github.com/redis/redis/blob/15b993f1ef6ec436c1c2f411232b27d150a6c168/src/ziplist.c#L284
// ziplist entry
// prevrawlensize
// prevrawlen
// lensize
// len
// headersize
// encoding 编码方式
// *p
func readZiplistEntry(bytes []byte) (string, int) {
	prevLen := bytes[0]
	if prevLen == 254 {
		//prev_length := readUnsignedInt(reader)
	}
	entryHeader := bytes[1]
	if entryHeader>>6 == 0 {
		length := entryHeader & 0x3F
		value := bytes[2 : 2+length]
		return string(value), int(2 + length)
	} else if entryHeader>>6 == 1 {
		length := entryHeader&0x3F | bytes[2]
		value := bytes[3 : 3+length]
		return string(value), int(3 + length)
	} else if entryHeader>>6 == 2 {
		fmt.Println("6 ==2")
		//length := readUnsignedIntBe(reader)
		//value := make([]byte, length)
		//fmt.Println(value)
	} else if entryHeader>>4 == 12 {
		fmt.Println("4 == 12")
		//length := readSignedShort(reader)
		//value := make([]byte, length)
		//fmt.Println(value)
	} else if entryHeader>>4 == 13 {
		value := int32(binary.LittleEndian.Uint32(bytes[2:6]))
		return strconv.FormatInt(int64(value), 10), 6
	} else if entryHeader>>4 == 14 {
		value := int64(binary.LittleEndian.Uint64(bytes[2:10]))
		return strconv.FormatInt(value, 10), 10
	} else if entryHeader == 240 {
		fmt.Println("240")

	} else if entryHeader == 254 {
		b := bytes[2]
		return strconv.Itoa(int(b)), 3
	} else if entryHeader >= 241 && entryHeader <= 253 {
		return strconv.Itoa(int(entryHeader - 241)), 2
	} else {
		panic("Invalid entry_header")
	}
	return "", 0

}

func readListFromQuicklist(reader *bufio.Reader) []string {
	count := readLength(reader)
	totalSize := 0
	elements := make([]string, 0)
	//fmt.Println("count", count)
	for i := 0; i < int(count); i++ {
		rawString := readString(reader)
		totalSize += len(rawString)
		bytes := []byte(rawString)
		// 存储压缩列表的总字节数
		//zlbytes := binary.LittleEndian.Uint32(bytes[:4])
		//tail_offset := binary.LittleEndian.Uint32(bytes[4:8])
		//fmt.Println("zlbytes", zlbytes, "tail_offset", tail_offset)
		numEntries := binary.LittleEndian.Uint16(bytes[8:10])
		bytes = bytes[10:]

		for i := 0; i < int(numEntries); i++ {
			entry, index := readZiplistEntry(bytes)
			bytes = bytes[index:]
			elements = append(elements, entry)
		}
	}
	return elements
}

func main() {
	fileName := "/Users/xiangzheng/dump.rdb"
	//fileName := "/Users/xiangzheng/Downloads/hins22112154_data_20231207012206.rdb"
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println(err)
		return
	}

	defer file.Close()
	reader := bufio.NewReaderSize(file, 50*1024*1024)

	// REDIS0009
	magicString := make([]byte, 9)
	file.Read(magicString)
	fmt.Println(string(magicString))

	keys := make([]RedisKey, 0)
	var db uint = 0

	for {
		b, err := reader.ReadByte()
		if b == Aux {
			b, _ := reader.ReadByte()
			bytes := make([]byte, b)
			reader.Read(bytes)
			value := readString(reader)
			fmt.Println(string(bytes), "", value)
		} else if b == SelectDb {
			dbNumber := readUnsignedChar(reader)
			fmt.Println("SelectDb: ", dbNumber)
			db = uint(dbNumber)
		} else if b == ResizeDd {
			dbSize := readLength(reader)
			expireSize := readLength(reader)
			fmt.Println("dbSize:", dbSize, "expireSize:", expireSize)
		} else if b == RedisRdbTypeListQuicklist {
			keyName := readKey(reader)
			elements := readListFromQuicklist(reader)
			fmt.Println("List key:", keyName, " 元素: ", elements)
			keys = append(keys, RedisKey{db, keyName, 0, 0, List, uint32(len(elements))})
		} else if b == RedisRdbTypeString {
			keyName := readKey(reader)
			value := readString(reader)
			//fmt.Println("Str key:", keyName, "value:", value)
			// 读取但不打印为了测试
			keys = append(keys, RedisKey{db, keyName, 0, uint32(len(value)), String, 1})
		} else if b == ExpiretimeMs {
			// 过期时间
			readMillisecondsTime(reader)
			//fmt.Println("过期时间", mts)
		} else if b == EXPIRETIME {
			fmt.Println("待解析过期时间")
		} else if b == EOF {
			crc32CheckSum := make([]byte, 8)
			reader.Read(crc32CheckSum)
			break
		} else if b == RedisRdbTypeZsetZiplist {
			keyName := readKey(reader)
			if keyName == "last_reported" {
				fmt.Println(keyName)
			}

			members := readZsetFromZiplist(reader)
			keys = append(keys, RedisKey{db, keyName, 0, 0, ZSet, uint32(len(members))})
			fmt.Println("ZSET(ziplist), key:", keyName, ", Value: ", members)
		} else if b == IDLE {
			// 返回idle的值，猜测可能是剩余秒数
			readLength(reader)
		} else if b == RedisRdbTypeSet {
			keyName := readKey(reader)
			length := readLength(reader)
			members := make([]string, 0)
			for i := 0; i < int(length); i++ {
				member := readString(reader)
				members = append(members, member)
			}
			fmt.Println("Set(hashtable) key:", keyName, "元素数量: ", length, ",元素: ", members)
			keys = append(keys, RedisKey{db, keyName, 0, 0, Set, length})
		} else if b == RedisRdbTypeSetIntset {
			keyName := readKey(reader)
			members := readIntSet(reader)
			fmt.Println("Set(int), key:", keyName, ", Value: ", members)
			keys = append(keys, RedisKey{db, keyName, 0, 0, Set, uint32(len(members))})
		} else if b == RedisRdbTypeZset2 || b == RedisRdbTypeZset {
			keyName := readKey(reader)
			membersLength := readLength(reader)
			fmt.Println("ZSET, key:", keyName, "元素数量:", membersLength)
			for i := 0; i < int(membersLength); i++ {
				readString(reader) // member
				if b == RedisRdbTypeZset2 {
					readBinaryDouble(reader) // score
					//fmt.Println("member:", member, ", score:", int(score))
				}
				if b == RedisRdbTypeZset {
				}
			}
			keys = append(keys, RedisKey{db, keyName, 0, 0, ZSet, membersLength})
		} else if b == RedisRdbTypeHashZiplist {
			keyName := readKey(reader)
			fmt.Println("Hash(ziplist), key:", keyName)
			nums := readHashFromZiplist(reader)
			keys = append(keys, RedisKey{db, keyName, 0, 0, Hash, uint32(nums)})
		} else if b == RedisRdbTypeHash {
			keyName := readKey(reader)
			length := readLength(reader)
			fmt.Print("Hash(hashtable), key:", keyName, "长度:", length)
			var count uint32 = 0
			for i := 0; i < int(length); i++ {
				field := readString(reader)
				value := readString(reader)
				fmt.Print(", field:", field, ", value:", value)
				count++
			}
			fmt.Println()
			keys = append(keys, RedisKey{db, keyName, 0, 0, Hash, count})
		} else if b == RedisRdbTypeStreamListpacks {
			keyName := readKey(reader)
			fmt.Println("待解析stream: ", keyName)
		} else {
			keyName := readKey(reader)
			fmt.Println("读取到未知类型, key:", keyName, "类型:", b)
			break
		}

		if err == io.EOF {
			fmt.Println("Done")
			break
		}
	}

	fmt.Println("keys: ", keys)
	fmt.Println("keys数量: ", len(keys))
}
