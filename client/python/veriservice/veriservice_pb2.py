# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: veriservice.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='veriservice.proto',
  package='veriservice',
  syntax='proto3',
  serialized_pb=_b('\n\x11veriservice.proto\x12\x0bveriservice\"X\n\nKnnRequest\x12\n\n\x02id\x18\x01 \x01(\t\x12\x11\n\ttimestamp\x18\x02 \x01(\x03\x12\x0f\n\x07timeout\x18\x03 \x01(\x03\x12\t\n\x01k\x18\x04 \x01(\x05\x12\x0f\n\x07\x66\x65\x61ture\x18\x05 \x03(\x01\"\x15\n\x13GetLocalDataRequest\"P\n\x07\x46\x65\x61ture\x12\x0f\n\x07\x66\x65\x61ture\x18\x01 \x03(\x01\x12\x11\n\ttimestamp\x18\x02 \x01(\x03\x12\r\n\x05label\x18\x03 \x01(\t\x12\x12\n\ngrouplabel\x18\x04 \x01(\t\"A\n\x0bKnnResponse\x12\n\n\x02id\x18\x01 \x01(\t\x12&\n\x08\x66\x65\x61tures\x18\x02 \x03(\x0b\x32\x14.veriservice.Feature\"Y\n\x10InsertionRequest\x12\x11\n\ttimestamp\x18\x01 \x01(\x03\x12\r\n\x05label\x18\x02 \x01(\t\x12\x12\n\ngrouplabel\x18\x03 \x01(\t\x12\x0f\n\x07\x66\x65\x61ture\x18\x04 \x03(\x01\"!\n\x11InsertionResponse\x12\x0c\n\x04\x63ode\x18\x02 \x01(\x05\"\x1b\n\nGetRequest\x12\r\n\x05label\x18\x02 \x01(\t\",\n\x0bGetResponse\x12\x0c\n\x04\x63ode\x18\x01 \x01(\x05\x12\x0f\n\x07\x66\x65\x61ture\x18\x05 \x03(\x01\"\"\n\x0eServiceMessage\x12\x10\n\x08services\x18\x01 \x03(\t\"a\n\x04Peer\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x11\n\ttimestamp\x18\x02 \x01(\x03\x12\x0f\n\x07version\x18\x03 \x01(\t\x12\x0b\n\x03\x61vg\x18\x04 \x03(\x01\x12\x0c\n\x04hist\x18\x05 \x03(\x01\x12\t\n\x01n\x18\x06 \x01(\x03\"/\n\x0bPeerMessage\x12 \n\x05peers\x18\x01 \x03(\x0b\x32\x11.veriservice.Peer\"v\n\x0bJoinRequest\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t\x12\x0c\n\x04port\x18\x02 \x01(\x05\x12\x0b\n\x03\x61vg\x18\x03 \x03(\x01\x12\x0f\n\x07version\x18\x04 \x01(\t\x12\x0c\n\x04hist\x18\x05 \x03(\x01\x12\t\n\x01n\x18\x06 \x01(\x03\x12\x11\n\ttimestamp\x18\x07 \x01(\x03\"\x1f\n\x0cJoinResponse\x12\x0f\n\x07\x61\x64\x64ress\x18\x01 \x01(\t2\xf5\x03\n\x0bVeriService\x12=\n\x06GetKnn\x12\x17.veriservice.KnnRequest\x1a\x18.veriservice.KnnResponse\"\x00\x12I\n\x06Insert\x12\x1d.veriservice.InsertionRequest\x1a\x1e.veriservice.InsertionResponse\"\x00\x12:\n\x03Get\x12\x17.veriservice.GetRequest\x1a\x18.veriservice.GetResponse\"\x00\x12=\n\x04Join\x12\x18.veriservice.JoinRequest\x1a\x19.veriservice.JoinResponse\"\x00\x12N\n\x10\x45xchangeServices\x12\x1b.veriservice.ServiceMessage\x1a\x1b.veriservice.ServiceMessage\"\x00\x12\x45\n\rExchangePeers\x12\x18.veriservice.PeerMessage\x1a\x18.veriservice.PeerMessage\"\x00\x12J\n\x0cGetLocalData\x12 .veriservice.GetLocalDataRequest\x1a\x14.veriservice.Feature\"\x00\x30\x01\x62\x06proto3')
)




_KNNREQUEST = _descriptor.Descriptor(
  name='KnnRequest',
  full_name='veriservice.KnnRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='veriservice.KnnRequest.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='veriservice.KnnRequest.timestamp', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='timeout', full_name='veriservice.KnnRequest.timeout', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='k', full_name='veriservice.KnnRequest.k', index=3,
      number=4, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='feature', full_name='veriservice.KnnRequest.feature', index=4,
      number=5, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=34,
  serialized_end=122,
)


_GETLOCALDATAREQUEST = _descriptor.Descriptor(
  name='GetLocalDataRequest',
  full_name='veriservice.GetLocalDataRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=124,
  serialized_end=145,
)


_FEATURE = _descriptor.Descriptor(
  name='Feature',
  full_name='veriservice.Feature',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='feature', full_name='veriservice.Feature.feature', index=0,
      number=1, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='veriservice.Feature.timestamp', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='label', full_name='veriservice.Feature.label', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='grouplabel', full_name='veriservice.Feature.grouplabel', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=147,
  serialized_end=227,
)


_KNNRESPONSE = _descriptor.Descriptor(
  name='KnnResponse',
  full_name='veriservice.KnnResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='veriservice.KnnResponse.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='features', full_name='veriservice.KnnResponse.features', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=229,
  serialized_end=294,
)


_INSERTIONREQUEST = _descriptor.Descriptor(
  name='InsertionRequest',
  full_name='veriservice.InsertionRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='veriservice.InsertionRequest.timestamp', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='label', full_name='veriservice.InsertionRequest.label', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='grouplabel', full_name='veriservice.InsertionRequest.grouplabel', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='feature', full_name='veriservice.InsertionRequest.feature', index=3,
      number=4, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=296,
  serialized_end=385,
)


_INSERTIONRESPONSE = _descriptor.Descriptor(
  name='InsertionResponse',
  full_name='veriservice.InsertionResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='code', full_name='veriservice.InsertionResponse.code', index=0,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=387,
  serialized_end=420,
)


_GETREQUEST = _descriptor.Descriptor(
  name='GetRequest',
  full_name='veriservice.GetRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='label', full_name='veriservice.GetRequest.label', index=0,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=422,
  serialized_end=449,
)


_GETRESPONSE = _descriptor.Descriptor(
  name='GetResponse',
  full_name='veriservice.GetResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='code', full_name='veriservice.GetResponse.code', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='feature', full_name='veriservice.GetResponse.feature', index=1,
      number=5, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=451,
  serialized_end=495,
)


_SERVICEMESSAGE = _descriptor.Descriptor(
  name='ServiceMessage',
  full_name='veriservice.ServiceMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='services', full_name='veriservice.ServiceMessage.services', index=0,
      number=1, type=9, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=497,
  serialized_end=531,
)


_PEER = _descriptor.Descriptor(
  name='Peer',
  full_name='veriservice.Peer',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='address', full_name='veriservice.Peer.address', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='veriservice.Peer.timestamp', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='version', full_name='veriservice.Peer.version', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='avg', full_name='veriservice.Peer.avg', index=3,
      number=4, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='hist', full_name='veriservice.Peer.hist', index=4,
      number=5, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='n', full_name='veriservice.Peer.n', index=5,
      number=6, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=533,
  serialized_end=630,
)


_PEERMESSAGE = _descriptor.Descriptor(
  name='PeerMessage',
  full_name='veriservice.PeerMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='peers', full_name='veriservice.PeerMessage.peers', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=632,
  serialized_end=679,
)


_JOINREQUEST = _descriptor.Descriptor(
  name='JoinRequest',
  full_name='veriservice.JoinRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='address', full_name='veriservice.JoinRequest.address', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='port', full_name='veriservice.JoinRequest.port', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='avg', full_name='veriservice.JoinRequest.avg', index=2,
      number=3, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='version', full_name='veriservice.JoinRequest.version', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='hist', full_name='veriservice.JoinRequest.hist', index=4,
      number=5, type=1, cpp_type=5, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='n', full_name='veriservice.JoinRequest.n', index=5,
      number=6, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='veriservice.JoinRequest.timestamp', index=6,
      number=7, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=681,
  serialized_end=799,
)


_JOINRESPONSE = _descriptor.Descriptor(
  name='JoinResponse',
  full_name='veriservice.JoinResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='address', full_name='veriservice.JoinResponse.address', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=801,
  serialized_end=832,
)

_KNNRESPONSE.fields_by_name['features'].message_type = _FEATURE
_PEERMESSAGE.fields_by_name['peers'].message_type = _PEER
DESCRIPTOR.message_types_by_name['KnnRequest'] = _KNNREQUEST
DESCRIPTOR.message_types_by_name['GetLocalDataRequest'] = _GETLOCALDATAREQUEST
DESCRIPTOR.message_types_by_name['Feature'] = _FEATURE
DESCRIPTOR.message_types_by_name['KnnResponse'] = _KNNRESPONSE
DESCRIPTOR.message_types_by_name['InsertionRequest'] = _INSERTIONREQUEST
DESCRIPTOR.message_types_by_name['InsertionResponse'] = _INSERTIONRESPONSE
DESCRIPTOR.message_types_by_name['GetRequest'] = _GETREQUEST
DESCRIPTOR.message_types_by_name['GetResponse'] = _GETRESPONSE
DESCRIPTOR.message_types_by_name['ServiceMessage'] = _SERVICEMESSAGE
DESCRIPTOR.message_types_by_name['Peer'] = _PEER
DESCRIPTOR.message_types_by_name['PeerMessage'] = _PEERMESSAGE
DESCRIPTOR.message_types_by_name['JoinRequest'] = _JOINREQUEST
DESCRIPTOR.message_types_by_name['JoinResponse'] = _JOINRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

KnnRequest = _reflection.GeneratedProtocolMessageType('KnnRequest', (_message.Message,), dict(
  DESCRIPTOR = _KNNREQUEST,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.KnnRequest)
  ))
_sym_db.RegisterMessage(KnnRequest)

GetLocalDataRequest = _reflection.GeneratedProtocolMessageType('GetLocalDataRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETLOCALDATAREQUEST,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.GetLocalDataRequest)
  ))
_sym_db.RegisterMessage(GetLocalDataRequest)

Feature = _reflection.GeneratedProtocolMessageType('Feature', (_message.Message,), dict(
  DESCRIPTOR = _FEATURE,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.Feature)
  ))
_sym_db.RegisterMessage(Feature)

KnnResponse = _reflection.GeneratedProtocolMessageType('KnnResponse', (_message.Message,), dict(
  DESCRIPTOR = _KNNRESPONSE,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.KnnResponse)
  ))
_sym_db.RegisterMessage(KnnResponse)

InsertionRequest = _reflection.GeneratedProtocolMessageType('InsertionRequest', (_message.Message,), dict(
  DESCRIPTOR = _INSERTIONREQUEST,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.InsertionRequest)
  ))
_sym_db.RegisterMessage(InsertionRequest)

InsertionResponse = _reflection.GeneratedProtocolMessageType('InsertionResponse', (_message.Message,), dict(
  DESCRIPTOR = _INSERTIONRESPONSE,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.InsertionResponse)
  ))
_sym_db.RegisterMessage(InsertionResponse)

GetRequest = _reflection.GeneratedProtocolMessageType('GetRequest', (_message.Message,), dict(
  DESCRIPTOR = _GETREQUEST,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.GetRequest)
  ))
_sym_db.RegisterMessage(GetRequest)

GetResponse = _reflection.GeneratedProtocolMessageType('GetResponse', (_message.Message,), dict(
  DESCRIPTOR = _GETRESPONSE,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.GetResponse)
  ))
_sym_db.RegisterMessage(GetResponse)

ServiceMessage = _reflection.GeneratedProtocolMessageType('ServiceMessage', (_message.Message,), dict(
  DESCRIPTOR = _SERVICEMESSAGE,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.ServiceMessage)
  ))
_sym_db.RegisterMessage(ServiceMessage)

Peer = _reflection.GeneratedProtocolMessageType('Peer', (_message.Message,), dict(
  DESCRIPTOR = _PEER,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.Peer)
  ))
_sym_db.RegisterMessage(Peer)

PeerMessage = _reflection.GeneratedProtocolMessageType('PeerMessage', (_message.Message,), dict(
  DESCRIPTOR = _PEERMESSAGE,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.PeerMessage)
  ))
_sym_db.RegisterMessage(PeerMessage)

JoinRequest = _reflection.GeneratedProtocolMessageType('JoinRequest', (_message.Message,), dict(
  DESCRIPTOR = _JOINREQUEST,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.JoinRequest)
  ))
_sym_db.RegisterMessage(JoinRequest)

JoinResponse = _reflection.GeneratedProtocolMessageType('JoinResponse', (_message.Message,), dict(
  DESCRIPTOR = _JOINRESPONSE,
  __module__ = 'veriservice_pb2'
  # @@protoc_insertion_point(class_scope:veriservice.JoinResponse)
  ))
_sym_db.RegisterMessage(JoinResponse)



_VERISERVICE = _descriptor.ServiceDescriptor(
  name='VeriService',
  full_name='veriservice.VeriService',
  file=DESCRIPTOR,
  index=0,
  options=None,
  serialized_start=835,
  serialized_end=1336,
  methods=[
  _descriptor.MethodDescriptor(
    name='GetKnn',
    full_name='veriservice.VeriService.GetKnn',
    index=0,
    containing_service=None,
    input_type=_KNNREQUEST,
    output_type=_KNNRESPONSE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='Insert',
    full_name='veriservice.VeriService.Insert',
    index=1,
    containing_service=None,
    input_type=_INSERTIONREQUEST,
    output_type=_INSERTIONRESPONSE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='Get',
    full_name='veriservice.VeriService.Get',
    index=2,
    containing_service=None,
    input_type=_GETREQUEST,
    output_type=_GETRESPONSE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='Join',
    full_name='veriservice.VeriService.Join',
    index=3,
    containing_service=None,
    input_type=_JOINREQUEST,
    output_type=_JOINRESPONSE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='ExchangeServices',
    full_name='veriservice.VeriService.ExchangeServices',
    index=4,
    containing_service=None,
    input_type=_SERVICEMESSAGE,
    output_type=_SERVICEMESSAGE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='ExchangePeers',
    full_name='veriservice.VeriService.ExchangePeers',
    index=5,
    containing_service=None,
    input_type=_PEERMESSAGE,
    output_type=_PEERMESSAGE,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='GetLocalData',
    full_name='veriservice.VeriService.GetLocalData',
    index=6,
    containing_service=None,
    input_type=_GETLOCALDATAREQUEST,
    output_type=_FEATURE,
    options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_VERISERVICE)

DESCRIPTOR.services_by_name['VeriService'] = _VERISERVICE

# @@protoc_insertion_point(module_scope)
