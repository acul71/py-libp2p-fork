# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: rpc.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\trpc.proto\x12\tpubsub.pb\"\xb4\x01\n\x03RPC\x12-\n\rsubscriptions\x18\x01 \x03(\x0b\x32\x16.pubsub.pb.RPC.SubOpts\x12#\n\x07publish\x18\x02 \x03(\x0b\x32\x12.pubsub.pb.Message\x12*\n\x07\x63ontrol\x18\x03 \x01(\x0b\x32\x19.pubsub.pb.ControlMessage\x1a-\n\x07SubOpts\x12\x11\n\tsubscribe\x18\x01 \x01(\x08\x12\x0f\n\x07topicid\x18\x02 \x01(\t\"i\n\x07Message\x12\x0f\n\x07\x66rom_id\x18\x01 \x01(\x0c\x12\x0c\n\x04\x64\x61ta\x18\x02 \x01(\x0c\x12\r\n\x05seqno\x18\x03 \x01(\x0c\x12\x10\n\x08topicIDs\x18\x04 \x03(\t\x12\x11\n\tsignature\x18\x05 \x01(\x0c\x12\x0b\n\x03key\x18\x06 \x01(\x0c\"\xb0\x01\n\x0e\x43ontrolMessage\x12&\n\x05ihave\x18\x01 \x03(\x0b\x32\x17.pubsub.pb.ControlIHave\x12&\n\x05iwant\x18\x02 \x03(\x0b\x32\x17.pubsub.pb.ControlIWant\x12&\n\x05graft\x18\x03 \x03(\x0b\x32\x17.pubsub.pb.ControlGraft\x12&\n\x05prune\x18\x04 \x03(\x0b\x32\x17.pubsub.pb.ControlPrune\"3\n\x0c\x43ontrolIHave\x12\x0f\n\x07topicID\x18\x01 \x01(\t\x12\x12\n\nmessageIDs\x18\x02 \x03(\t\"\"\n\x0c\x43ontrolIWant\x12\x12\n\nmessageIDs\x18\x01 \x03(\t\"\x1f\n\x0c\x43ontrolGraft\x12\x0f\n\x07topicID\x18\x01 \x01(\t\"T\n\x0c\x43ontrolPrune\x12\x0f\n\x07topicID\x18\x01 \x01(\t\x12\"\n\x05peers\x18\x02 \x03(\x0b\x32\x13.pubsub.pb.PeerInfo\x12\x0f\n\x07\x62\x61\x63koff\x18\x03 \x01(\x04\"4\n\x08PeerInfo\x12\x0e\n\x06peerID\x18\x01 \x01(\x0c\x12\x18\n\x10signedPeerRecord\x18\x02 \x01(\x0c\"\x87\x03\n\x0fTopicDescriptor\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x31\n\x04\x61uth\x18\x02 \x01(\x0b\x32#.pubsub.pb.TopicDescriptor.AuthOpts\x12/\n\x03\x65nc\x18\x03 \x01(\x0b\x32\".pubsub.pb.TopicDescriptor.EncOpts\x1a|\n\x08\x41uthOpts\x12:\n\x04mode\x18\x01 \x01(\x0e\x32,.pubsub.pb.TopicDescriptor.AuthOpts.AuthMode\x12\x0c\n\x04keys\x18\x02 \x03(\x0c\"&\n\x08\x41uthMode\x12\x08\n\x04NONE\x10\x00\x12\x07\n\x03KEY\x10\x01\x12\x07\n\x03WOT\x10\x02\x1a\x83\x01\n\x07\x45ncOpts\x12\x38\n\x04mode\x18\x01 \x01(\x0e\x32*.pubsub.pb.TopicDescriptor.EncOpts.EncMode\x12\x11\n\tkeyHashes\x18\x02 \x03(\x0c\"+\n\x07\x45ncMode\x12\x08\n\x04NONE\x10\x00\x12\r\n\tSHAREDKEY\x10\x01\x12\x07\n\x03WOT\x10\x02')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'rpc_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _RPC._serialized_start=25
  _RPC._serialized_end=205
  _RPC_SUBOPTS._serialized_start=160
  _RPC_SUBOPTS._serialized_end=205
  _MESSAGE._serialized_start=207
  _MESSAGE._serialized_end=312
  _CONTROLMESSAGE._serialized_start=315
  _CONTROLMESSAGE._serialized_end=491
  _CONTROLIHAVE._serialized_start=493
  _CONTROLIHAVE._serialized_end=544
  _CONTROLIWANT._serialized_start=546
  _CONTROLIWANT._serialized_end=580
  _CONTROLGRAFT._serialized_start=582
  _CONTROLGRAFT._serialized_end=613
  _CONTROLPRUNE._serialized_start=615
  _CONTROLPRUNE._serialized_end=699
  _PEERINFO._serialized_start=701
  _PEERINFO._serialized_end=753
  _TOPICDESCRIPTOR._serialized_start=756
  _TOPICDESCRIPTOR._serialized_end=1147
  _TOPICDESCRIPTOR_AUTHOPTS._serialized_start=889
  _TOPICDESCRIPTOR_AUTHOPTS._serialized_end=1013
  _TOPICDESCRIPTOR_AUTHOPTS_AUTHMODE._serialized_start=975
  _TOPICDESCRIPTOR_AUTHOPTS_AUTHMODE._serialized_end=1013
  _TOPICDESCRIPTOR_ENCOPTS._serialized_start=1016
  _TOPICDESCRIPTOR_ENCOPTS._serialized_end=1147
  _TOPICDESCRIPTOR_ENCOPTS_ENCMODE._serialized_start=1104
  _TOPICDESCRIPTOR_ENCOPTS_ENCMODE._serialized_end=1147
# @@protoc_insertion_point(module_scope)
