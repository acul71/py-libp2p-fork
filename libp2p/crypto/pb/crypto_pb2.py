# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: libp2p/crypto/pb/crypto.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'libp2p/crypto/pb/crypto.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1dlibp2p/crypto/pb/crypto.proto\x12\tcrypto.pb\"?\n\tPublicKey\x12$\n\x08key_type\x18\x01 \x02(\x0e\x32\x12.crypto.pb.KeyType\x12\x0c\n\x04\x64\x61ta\x18\x02 \x02(\x0c\"@\n\nPrivateKey\x12$\n\x08key_type\x18\x01 \x02(\x0e\x32\x12.crypto.pb.KeyType\x12\x0c\n\x04\x64\x61ta\x18\x02 \x02(\x0c*G\n\x07KeyType\x12\x07\n\x03RSA\x10\x00\x12\x0b\n\x07\x45\x64\x32\x35\x35\x31\x39\x10\x01\x12\r\n\tSecp256k1\x10\x02\x12\t\n\x05\x45\x43\x44SA\x10\x03\x12\x0c\n\x08\x45\x43\x43_P256\x10\x04')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'libp2p.crypto.pb.crypto_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_KEYTYPE']._serialized_start=175
  _globals['_KEYTYPE']._serialized_end=246
  _globals['_PUBLICKEY']._serialized_start=44
  _globals['_PUBLICKEY']._serialized_end=107
  _globals['_PRIVATEKEY']._serialized_start=109
  _globals['_PRIVATEKEY']._serialized_end=173
# @@protoc_insertion_point(module_scope)
