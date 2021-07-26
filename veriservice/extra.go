package veriservice

import protoimpl "google.golang.org/protobuf/runtime/protoimpl"

func (x *Datum) Clean() {
	x.sizeCache = protoimpl.SizeCache(0)
	x.state = protoimpl.MessageState{}
	x.unknownFields = protoimpl.UnknownFields{}
	if x.Key != nil {
		x.Key.Clean()
	}
	if x.Value != nil {
		x.Value.Clean()
	}
}

func (x *DatumKey) Clean() {
	x.sizeCache = protoimpl.SizeCache(0)
	x.state = protoimpl.MessageState{}
	x.unknownFields = protoimpl.UnknownFields{}
}

func (x *DatumValue) Clean() {
	x.sizeCache = protoimpl.SizeCache(0)
	x.state = protoimpl.MessageState{}
	x.unknownFields = protoimpl.UnknownFields{}
}
