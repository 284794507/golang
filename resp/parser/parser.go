package parser

import (
	"bufio"
	"bytes"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int
	msgType           byte
	args              [][]byte
	bulkLen           int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	for {
		line, err := bufReader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Err: err}
			close(ch)
			return
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			//there are some empty lines with in replication traffic, ignore this error
			//protocalError(ch,"empty line")
			continue
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		case '+':
			content := string(line[1:])
			ch <- &Payload{
				Data: reply.MakeStatusReply(content),
			}
			if strings.HasPrefix(content, "FULLRESYNC") {
				err = parseRDBBulkString(bufReader, ch)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					close(ch)
					return
				}
			}

		case '-':
			ch <- &Payload{
				Data: reply.MakeStandardErrReply(string(line[1:])),
			}
		case ':':
			value, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: reply.MakeIntReply(value),
			}
		case '$':
			err = parseBulking(line, bufReader, ch)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
		case '*':
			err = parseArray(line, bufReader, ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			args := bytes.Split(line, []byte{' '})
			ch <- &Payload{
				Data: reply.MakeMultiBulkReply(args),
			}
		}
	}
}

// there is no CRLF between RDB and following ADF, therefore it needs to be treated differently
func parseRDBBulkString(reader *bufio.Reader, ch chan<- *Payload) error {
	header, err := reader.ReadBytes('\n')
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	if len(header) == 0 {
		return errors.New("empty header")
	}
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen <= 0 {
		return errors.New("illegal bulk header:" + string(header))
	}
	body := make([]byte, strLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: reply.MakeBulkReply(body[:len(body)]),
	}
	return nil
}

func parseBulking(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64)
	if err != nil || strLen < -1 {
		protocolError(ch, "illegal bulk string header:"+string(header))
		return nil
	} else if strLen == -1 {
		ch <- &Payload{
			Data: reply.MakeNullBulkReply(),
		}
		return nil
	}
	body := make([]byte, strLen+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: reply.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

func parseArray(header []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	lenStr := string(header[1:])
	arrLen, err := strconv.Atoi(lenStr)
	if err != nil {
		return err
	}
	var args [][]byte
	for i := 0; i < arrLen; i++ {
		curLenByte, err := reader.ReadBytes('\n')
		curLenByte = bytes.TrimSuffix(curLenByte, []byte{'\r', '\n'})
		if err != nil {
			return err
		}
		curLen, err := strconv.Atoi(string(curLenByte[1:]))
		if err != nil {
			return err
		}
		curVal, err := reader.ReadBytes('\n')
		curVal = bytes.TrimSuffix(curVal, []byte{'\r', '\n'})
		if err != nil {
			return err
		}
		if len(curVal) == curLen {
			args = append(args, curVal)
		}
	}
	ch <- &Payload{
		Data: reply.MakeMultiBulkReply(args),
	}
	return nil
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error:" + msg)
	ch <- &Payload{Err: err}
}
