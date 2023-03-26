package reply

func getFormatStr(str string) string {
	return "-" + str + "\r\n"
}

type UnKonwErrReply struct {
}

var errStr string = "Err unkonwn"
var unkownErrBytes = []byte(getFormatStr(errStr))

func (u UnKonwErrReply) Error() string {
	return errStr
}

func (u UnKonwErrReply) ToBytes() []byte {
	return unkownErrBytes
}

type ArgNumErrReply struct {
	Cmd string
}

var argNumStr string = "ERR wrong number of arguments for "

func (a ArgNumErrReply) Error() string {
	return argNumStr + a.Cmd
}

func (a *ArgNumErrReply) ToBytes() []byte {
	return []byte(getFormatStr(argNumStr + a.Cmd))
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

type SyntaxErrReply struct {
}

var syntaxErr string = "Err syntax error"
var syntaxErrBytes = []byte(getFormatStr(syntaxErr))
var theSyntaxErrReply = &SyntaxErrReply{}

func MakeSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func (r *SyntaxErrReply) Error() string {
	return syntaxErr
}

type WrongTypeErrReply struct {
}

var wrongTypeErr string = "WRONGTYPE opration against a key holding the kind of value"
var wrongTypeErrBytes = []byte(getFormatStr(wrongTypeErr))
var theWrongTypeErrReply = &WrongTypeErrReply{}

func MakeWrongTypeErrReply() *WrongTypeErrReply {
	return theWrongTypeErrReply
}

func (r *WrongTypeErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return wrongTypeErr
}

type ProtocolErrReply struct {
	Msg string
}

var protocolErrStr string = "Err protocol error:'"
var protocolErrBytes = []byte(getFormatStr(protocolErrStr))

func (p ProtocolErrReply) Error() string {
	return protocolErrStr + p.Msg
}

func (p ProtocolErrReply) ToBytes() []byte {
	return protocolErrBytes
}
