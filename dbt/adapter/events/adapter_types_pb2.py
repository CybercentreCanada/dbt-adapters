# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: adapter_types.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x13\x61\x64\x61pter_types.proto\x12\x0bproto_types\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1cgoogle/protobuf/struct.proto"\xab\x02\n\x16\x41\x64\x61pterCommonEventInfo\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04\x63ode\x18\x02 \x01(\t\x12\x0b\n\x03msg\x18\x03 \x01(\t\x12\r\n\x05level\x18\x04 \x01(\t\x12\x15\n\rinvocation_id\x18\x05 \x01(\t\x12\x0b\n\x03pid\x18\x06 \x01(\x05\x12\x0e\n\x06thread\x18\x07 \x01(\t\x12&\n\x02ts\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12=\n\x05\x65xtra\x18\t \x03(\x0b\x32..proto_types.AdapterCommonEventInfo.ExtraEntry\x12\x10\n\x08\x63\x61tegory\x18\n \x01(\t\x1a,\n\nExtraEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01"]\n\x13\x41\x64\x61pterNodeRelation\x12\x10\n\x08\x64\x61tabase\x18\n \x01(\t\x12\x0e\n\x06schema\x18\x0b \x01(\t\x12\r\n\x05\x61lias\x18\x0c \x01(\t\x12\x15\n\rrelation_name\x18\r \x01(\t"\x9f\x02\n\x0f\x41\x64\x61pterNodeInfo\x12\x11\n\tnode_path\x18\x01 \x01(\t\x12\x11\n\tnode_name\x18\x02 \x01(\t\x12\x11\n\tunique_id\x18\x03 \x01(\t\x12\x15\n\rresource_type\x18\x04 \x01(\t\x12\x14\n\x0cmaterialized\x18\x05 \x01(\t\x12\x13\n\x0bnode_status\x18\x06 \x01(\t\x12\x17\n\x0fnode_started_at\x18\x07 \x01(\t\x12\x18\n\x10node_finished_at\x18\x08 \x01(\t\x12%\n\x04meta\x18\t \x01(\x0b\x32\x17.google.protobuf.Struct\x12\x37\n\rnode_relation\x18\n \x01(\x0b\x32 .proto_types.AdapterNodeRelation"G\n\x0fReferenceKeyMsg\x12\x10\n\x08\x64\x61tabase\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12\x12\n\nidentifier\x18\x03 \x01(\t"?\n\x19\x41\x64\x61pterDeprecationWarning\x12\x10\n\x08old_name\x18\x01 \x01(\t\x12\x10\n\x08new_name\x18\x02 \x01(\t"\x87\x01\n\x1c\x41\x64\x61pterDeprecationWarningMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\x34\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32&.proto_types.AdapterDeprecationWarning"!\n\x1f\x43ollectFreshnessReturnSignature"\x93\x01\n"CollectFreshnessReturnSignatureMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12:\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32,.proto_types.CollectFreshnessReturnSignature"\x8e\x01\n\x11\x41\x64\x61pterEventDebug\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x10\n\x08\x62\x61se_msg\x18\x03 \x01(\t\x12(\n\x04\x61rgs\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.ListValue"w\n\x14\x41\x64\x61pterEventDebugMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12,\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1e.proto_types.AdapterEventDebug"\x8d\x01\n\x10\x41\x64\x61pterEventInfo\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x10\n\x08\x62\x61se_msg\x18\x03 \x01(\t\x12(\n\x04\x61rgs\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.ListValue"u\n\x13\x41\x64\x61pterEventInfoMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12+\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1d.proto_types.AdapterEventInfo"\x90\x01\n\x13\x41\x64\x61pterEventWarning\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x10\n\x08\x62\x61se_msg\x18\x03 \x01(\t\x12(\n\x04\x61rgs\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.ListValue"{\n\x16\x41\x64\x61pterEventWarningMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12.\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32 .proto_types.AdapterEventWarning"\xa0\x01\n\x11\x41\x64\x61pterEventError\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x10\n\x08\x62\x61se_msg\x18\x03 \x01(\t\x12(\n\x04\x61rgs\x18\x04 \x01(\x0b\x32\x1a.google.protobuf.ListValue\x12\x10\n\x08\x65xc_info\x18\x05 \x01(\t"w\n\x14\x41\x64\x61pterEventErrorMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12,\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1e.proto_types.AdapterEventError"f\n\rNewConnection\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_type\x18\x02 \x01(\t\x12\x11\n\tconn_name\x18\x03 \x01(\t"o\n\x10NewConnectionMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12(\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1a.proto_types.NewConnection"=\n\x10\x43onnectionReused\x12\x11\n\tconn_name\x18\x01 \x01(\t\x12\x16\n\x0eorig_conn_name\x18\x02 \x01(\t"u\n\x13\x43onnectionReusedMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12+\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1d.proto_types.ConnectionReused"0\n\x1b\x43onnectionLeftOpenInCleanup\x12\x11\n\tconn_name\x18\x01 \x01(\t"\x8b\x01\n\x1e\x43onnectionLeftOpenInCleanupMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\x36\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32(.proto_types.ConnectionLeftOpenInCleanup".\n\x19\x43onnectionClosedInCleanup\x12\x11\n\tconn_name\x18\x01 \x01(\t"\x87\x01\n\x1c\x43onnectionClosedInCleanupMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\x34\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32&.proto_types.ConnectionClosedInCleanup"f\n\x0eRollbackFailed\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_name\x18\x02 \x01(\t\x12\x10\n\x08\x65xc_info\x18\x03 \x01(\t"q\n\x11RollbackFailedMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.RollbackFailed"V\n\x10\x43onnectionClosed\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_name\x18\x02 \x01(\t"u\n\x13\x43onnectionClosedMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12+\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1d.proto_types.ConnectionClosed"X\n\x12\x43onnectionLeftOpen\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_name\x18\x02 \x01(\t"y\n\x15\x43onnectionLeftOpenMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.ConnectionLeftOpen"N\n\x08Rollback\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_name\x18\x02 \x01(\t"e\n\x0bRollbackMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12#\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x15.proto_types.Rollback"@\n\tCacheMiss\x12\x11\n\tconn_name\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\t\x12\x0e\n\x06schema\x18\x03 \x01(\t"g\n\x0c\x43\x61\x63heMissMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12$\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x16.proto_types.CacheMiss"b\n\rListRelations\x12\x10\n\x08\x64\x61tabase\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x02 \x01(\t\x12/\n\trelations\x18\x03 \x03(\x0b\x32\x1c.proto_types.ReferenceKeyMsg"o\n\x10ListRelationsMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12(\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1a.proto_types.ListRelations"g\n\x0e\x43onnectionUsed\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_type\x18\x02 \x01(\t\x12\x11\n\tconn_name\x18\x03 \x01(\t"q\n\x11\x43onnectionUsedMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.ConnectionUsed"[\n\x08SQLQuery\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_name\x18\x02 \x01(\t\x12\x0b\n\x03sql\x18\x03 \x01(\t"e\n\x0bSQLQueryMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12#\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x15.proto_types.SQLQuery"b\n\x0eSQLQueryStatus\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x0e\n\x06status\x18\x02 \x01(\t\x12\x0f\n\x07\x65lapsed\x18\x03 \x01(\x02"q\n\x11SQLQueryStatusMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.SQLQueryStatus"O\n\tSQLCommit\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x11\n\tconn_name\x18\x02 \x01(\t"g\n\x0cSQLCommitMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12$\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x16.proto_types.SQLCommit"a\n\rColTypeChange\x12\x11\n\torig_type\x18\x01 \x01(\t\x12\x10\n\x08new_type\x18\x02 \x01(\t\x12+\n\x05table\x18\x03 \x01(\x0b\x32\x1c.proto_types.ReferenceKeyMsg"o\n\x10\x43olTypeChangeMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12(\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1a.proto_types.ColTypeChange"@\n\x0eSchemaCreation\x12.\n\x08relation\x18\x01 \x01(\x0b\x32\x1c.proto_types.ReferenceKeyMsg"q\n\x11SchemaCreationMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.SchemaCreation"<\n\nSchemaDrop\x12.\n\x08relation\x18\x01 \x01(\x0b\x32\x1c.proto_types.ReferenceKeyMsg"i\n\rSchemaDropMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12%\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x17.proto_types.SchemaDrop"\xde\x01\n\x0b\x43\x61\x63heAction\x12\x0e\n\x06\x61\x63tion\x18\x01 \x01(\t\x12-\n\x07ref_key\x18\x02 \x01(\x0b\x32\x1c.proto_types.ReferenceKeyMsg\x12/\n\tref_key_2\x18\x03 \x01(\x0b\x32\x1c.proto_types.ReferenceKeyMsg\x12/\n\tref_key_3\x18\x04 \x01(\x0b\x32\x1c.proto_types.ReferenceKeyMsg\x12.\n\x08ref_list\x18\x05 \x03(\x0b\x32\x1c.proto_types.ReferenceKeyMsg"k\n\x0e\x43\x61\x63heActionMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12&\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x18.proto_types.CacheAction"\x98\x01\n\x0e\x43\x61\x63heDumpGraph\x12\x33\n\x04\x64ump\x18\x01 \x03(\x0b\x32%.proto_types.CacheDumpGraph.DumpEntry\x12\x14\n\x0c\x62\x65\x66ore_after\x18\x02 \x01(\t\x12\x0e\n\x06\x61\x63tion\x18\x03 \x01(\t\x1a+\n\tDumpEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01"q\n\x11\x43\x61\x63heDumpGraphMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.CacheDumpGraph"B\n\x11\x41\x64\x61pterRegistered\x12\x14\n\x0c\x61\x64\x61pter_name\x18\x01 \x01(\t\x12\x17\n\x0f\x61\x64\x61pter_version\x18\x02 \x01(\t"w\n\x14\x41\x64\x61pterRegisteredMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12,\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1e.proto_types.AdapterRegistered"!\n\x12\x41\x64\x61pterImportError\x12\x0b\n\x03\x65xc\x18\x01 \x01(\t"y\n\x15\x41\x64\x61pterImportErrorMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.AdapterImportError"#\n\x0fPluginLoadError\x12\x10\n\x08\x65xc_info\x18\x01 \x01(\t"s\n\x12PluginLoadErrorMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12*\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1c.proto_types.PluginLoadError"a\n\x14NewConnectionOpening\x12/\n\tnode_info\x18\x01 \x01(\x0b\x32\x1c.proto_types.AdapterNodeInfo\x12\x18\n\x10\x63onnection_state\x18\x02 \x01(\t"}\n\x17NewConnectionOpeningMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12/\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32!.proto_types.NewConnectionOpening"8\n\rCodeExecution\x12\x11\n\tconn_name\x18\x01 \x01(\t\x12\x14\n\x0c\x63ode_content\x18\x02 \x01(\t"o\n\x10\x43odeExecutionMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12(\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1a.proto_types.CodeExecution"6\n\x13\x43odeExecutionStatus\x12\x0e\n\x06status\x18\x01 \x01(\t\x12\x0f\n\x07\x65lapsed\x18\x02 \x01(\x02"{\n\x16\x43odeExecutionStatusMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12.\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32 .proto_types.CodeExecutionStatus"%\n\x16\x43\x61talogGenerationError\x12\x0b\n\x03\x65xc\x18\x01 \x01(\t"\x81\x01\n\x19\x43\x61talogGenerationErrorMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\x31\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32#.proto_types.CatalogGenerationError"-\n\x13WriteCatalogFailure\x12\x16\n\x0enum_exceptions\x18\x01 \x01(\x05"{\n\x16WriteCatalogFailureMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12.\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32 .proto_types.WriteCatalogFailure"\x1e\n\x0e\x43\x61talogWritten\x12\x0c\n\x04path\x18\x01 \x01(\t"q\n\x11\x43\x61talogWrittenMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12)\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1b.proto_types.CatalogWritten"\x14\n\x12\x43\x61nnotGenerateDocs"y\n\x15\x43\x61nnotGenerateDocsMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12-\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1f.proto_types.CannotGenerateDocs"\x11\n\x0f\x42uildingCatalog"s\n\x12\x42uildingCatalogMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12*\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x1c.proto_types.BuildingCatalog"-\n\x18\x44\x61tabaseErrorRunningHook\x12\x11\n\thook_type\x18\x01 \x01(\t"\x85\x01\n\x1b\x44\x61tabaseErrorRunningHookMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\x33\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32%.proto_types.DatabaseErrorRunningHook"4\n\x0cHooksRunning\x12\x11\n\tnum_hooks\x18\x01 \x01(\x05\x12\x11\n\thook_type\x18\x02 \x01(\t"m\n\x0fHooksRunningMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\'\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32\x19.proto_types.HooksRunning"T\n\x14\x46inishedRunningStats\x12\x11\n\tstat_line\x18\x01 \x01(\t\x12\x11\n\texecution\x18\x02 \x01(\t\x12\x16\n\x0e\x65xecution_time\x18\x03 \x01(\x02"}\n\x17\x46inishedRunningStatsMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12/\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32!.proto_types.FinishedRunningStats"<\n\x15\x43onstraintNotEnforced\x12\x12\n\nconstraint\x18\x01 \x01(\t\x12\x0f\n\x07\x61\x64\x61pter\x18\x02 \x01(\t"\x7f\n\x18\x43onstraintNotEnforcedMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\x30\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32".proto_types.ConstraintNotEnforced"=\n\x16\x43onstraintNotSupported\x12\x12\n\nconstraint\x18\x01 \x01(\t\x12\x0f\n\x07\x61\x64\x61pter\x18\x02 \x01(\t"\x81\x01\n\x19\x43onstraintNotSupportedMsg\x12\x31\n\x04info\x18\x01 \x01(\x0b\x32#.proto_types.AdapterCommonEventInfo\x12\x31\n\x04\x64\x61ta\x18\x02 \x01(\x0b\x32#.proto_types.ConstraintNotSupportedb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "adapter_types_pb2", _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _ADAPTERCOMMONEVENTINFO_EXTRAENTRY._options = None
    _ADAPTERCOMMONEVENTINFO_EXTRAENTRY._serialized_options = b"8\001"
    _CACHEDUMPGRAPH_DUMPENTRY._options = None
    _CACHEDUMPGRAPH_DUMPENTRY._serialized_options = b"8\001"
    _globals["_ADAPTERCOMMONEVENTINFO"]._serialized_start = 100
    _globals["_ADAPTERCOMMONEVENTINFO"]._serialized_end = 399
    _globals["_ADAPTERCOMMONEVENTINFO_EXTRAENTRY"]._serialized_start = 355
    _globals["_ADAPTERCOMMONEVENTINFO_EXTRAENTRY"]._serialized_end = 399
    _globals["_ADAPTERNODERELATION"]._serialized_start = 401
    _globals["_ADAPTERNODERELATION"]._serialized_end = 494
    _globals["_ADAPTERNODEINFO"]._serialized_start = 497
    _globals["_ADAPTERNODEINFO"]._serialized_end = 784
    _globals["_REFERENCEKEYMSG"]._serialized_start = 786
    _globals["_REFERENCEKEYMSG"]._serialized_end = 857
    _globals["_ADAPTERDEPRECATIONWARNING"]._serialized_start = 859
    _globals["_ADAPTERDEPRECATIONWARNING"]._serialized_end = 922
    _globals["_ADAPTERDEPRECATIONWARNINGMSG"]._serialized_start = 925
    _globals["_ADAPTERDEPRECATIONWARNINGMSG"]._serialized_end = 1060
    _globals["_COLLECTFRESHNESSRETURNSIGNATURE"]._serialized_start = 1062
    _globals["_COLLECTFRESHNESSRETURNSIGNATURE"]._serialized_end = 1095
    _globals["_COLLECTFRESHNESSRETURNSIGNATUREMSG"]._serialized_start = 1098
    _globals["_COLLECTFRESHNESSRETURNSIGNATUREMSG"]._serialized_end = 1245
    _globals["_ADAPTEREVENTDEBUG"]._serialized_start = 1248
    _globals["_ADAPTEREVENTDEBUG"]._serialized_end = 1390
    _globals["_ADAPTEREVENTDEBUGMSG"]._serialized_start = 1392
    _globals["_ADAPTEREVENTDEBUGMSG"]._serialized_end = 1511
    _globals["_ADAPTEREVENTINFO"]._serialized_start = 1514
    _globals["_ADAPTEREVENTINFO"]._serialized_end = 1655
    _globals["_ADAPTEREVENTINFOMSG"]._serialized_start = 1657
    _globals["_ADAPTEREVENTINFOMSG"]._serialized_end = 1774
    _globals["_ADAPTEREVENTWARNING"]._serialized_start = 1777
    _globals["_ADAPTEREVENTWARNING"]._serialized_end = 1921
    _globals["_ADAPTEREVENTWARNINGMSG"]._serialized_start = 1923
    _globals["_ADAPTEREVENTWARNINGMSG"]._serialized_end = 2046
    _globals["_ADAPTEREVENTERROR"]._serialized_start = 2049
    _globals["_ADAPTEREVENTERROR"]._serialized_end = 2209
    _globals["_ADAPTEREVENTERRORMSG"]._serialized_start = 2211
    _globals["_ADAPTEREVENTERRORMSG"]._serialized_end = 2330
    _globals["_NEWCONNECTION"]._serialized_start = 2332
    _globals["_NEWCONNECTION"]._serialized_end = 2434
    _globals["_NEWCONNECTIONMSG"]._serialized_start = 2436
    _globals["_NEWCONNECTIONMSG"]._serialized_end = 2547
    _globals["_CONNECTIONREUSED"]._serialized_start = 2549
    _globals["_CONNECTIONREUSED"]._serialized_end = 2610
    _globals["_CONNECTIONREUSEDMSG"]._serialized_start = 2612
    _globals["_CONNECTIONREUSEDMSG"]._serialized_end = 2729
    _globals["_CONNECTIONLEFTOPENINCLEANUP"]._serialized_start = 2731
    _globals["_CONNECTIONLEFTOPENINCLEANUP"]._serialized_end = 2779
    _globals["_CONNECTIONLEFTOPENINCLEANUPMSG"]._serialized_start = 2782
    _globals["_CONNECTIONLEFTOPENINCLEANUPMSG"]._serialized_end = 2921
    _globals["_CONNECTIONCLOSEDINCLEANUP"]._serialized_start = 2923
    _globals["_CONNECTIONCLOSEDINCLEANUP"]._serialized_end = 2969
    _globals["_CONNECTIONCLOSEDINCLEANUPMSG"]._serialized_start = 2972
    _globals["_CONNECTIONCLOSEDINCLEANUPMSG"]._serialized_end = 3107
    _globals["_ROLLBACKFAILED"]._serialized_start = 3109
    _globals["_ROLLBACKFAILED"]._serialized_end = 3211
    _globals["_ROLLBACKFAILEDMSG"]._serialized_start = 3213
    _globals["_ROLLBACKFAILEDMSG"]._serialized_end = 3326
    _globals["_CONNECTIONCLOSED"]._serialized_start = 3328
    _globals["_CONNECTIONCLOSED"]._serialized_end = 3414
    _globals["_CONNECTIONCLOSEDMSG"]._serialized_start = 3416
    _globals["_CONNECTIONCLOSEDMSG"]._serialized_end = 3533
    _globals["_CONNECTIONLEFTOPEN"]._serialized_start = 3535
    _globals["_CONNECTIONLEFTOPEN"]._serialized_end = 3623
    _globals["_CONNECTIONLEFTOPENMSG"]._serialized_start = 3625
    _globals["_CONNECTIONLEFTOPENMSG"]._serialized_end = 3746
    _globals["_ROLLBACK"]._serialized_start = 3748
    _globals["_ROLLBACK"]._serialized_end = 3826
    _globals["_ROLLBACKMSG"]._serialized_start = 3828
    _globals["_ROLLBACKMSG"]._serialized_end = 3929
    _globals["_CACHEMISS"]._serialized_start = 3931
    _globals["_CACHEMISS"]._serialized_end = 3995
    _globals["_CACHEMISSMSG"]._serialized_start = 3997
    _globals["_CACHEMISSMSG"]._serialized_end = 4100
    _globals["_LISTRELATIONS"]._serialized_start = 4102
    _globals["_LISTRELATIONS"]._serialized_end = 4200
    _globals["_LISTRELATIONSMSG"]._serialized_start = 4202
    _globals["_LISTRELATIONSMSG"]._serialized_end = 4313
    _globals["_CONNECTIONUSED"]._serialized_start = 4315
    _globals["_CONNECTIONUSED"]._serialized_end = 4418
    _globals["_CONNECTIONUSEDMSG"]._serialized_start = 4420
    _globals["_CONNECTIONUSEDMSG"]._serialized_end = 4533
    _globals["_SQLQUERY"]._serialized_start = 4535
    _globals["_SQLQUERY"]._serialized_end = 4626
    _globals["_SQLQUERYMSG"]._serialized_start = 4628
    _globals["_SQLQUERYMSG"]._serialized_end = 4729
    _globals["_SQLQUERYSTATUS"]._serialized_start = 4731
    _globals["_SQLQUERYSTATUS"]._serialized_end = 4829
    _globals["_SQLQUERYSTATUSMSG"]._serialized_start = 4831
    _globals["_SQLQUERYSTATUSMSG"]._serialized_end = 4944
    _globals["_SQLCOMMIT"]._serialized_start = 4946
    _globals["_SQLCOMMIT"]._serialized_end = 5025
    _globals["_SQLCOMMITMSG"]._serialized_start = 5027
    _globals["_SQLCOMMITMSG"]._serialized_end = 5130
    _globals["_COLTYPECHANGE"]._serialized_start = 5132
    _globals["_COLTYPECHANGE"]._serialized_end = 5229
    _globals["_COLTYPECHANGEMSG"]._serialized_start = 5231
    _globals["_COLTYPECHANGEMSG"]._serialized_end = 5342
    _globals["_SCHEMACREATION"]._serialized_start = 5344
    _globals["_SCHEMACREATION"]._serialized_end = 5408
    _globals["_SCHEMACREATIONMSG"]._serialized_start = 5410
    _globals["_SCHEMACREATIONMSG"]._serialized_end = 5523
    _globals["_SCHEMADROP"]._serialized_start = 5525
    _globals["_SCHEMADROP"]._serialized_end = 5585
    _globals["_SCHEMADROPMSG"]._serialized_start = 5587
    _globals["_SCHEMADROPMSG"]._serialized_end = 5692
    _globals["_CACHEACTION"]._serialized_start = 5695
    _globals["_CACHEACTION"]._serialized_end = 5917
    _globals["_CACHEACTIONMSG"]._serialized_start = 5919
    _globals["_CACHEACTIONMSG"]._serialized_end = 6026
    _globals["_CACHEDUMPGRAPH"]._serialized_start = 6029
    _globals["_CACHEDUMPGRAPH"]._serialized_end = 6181
    _globals["_CACHEDUMPGRAPH_DUMPENTRY"]._serialized_start = 6138
    _globals["_CACHEDUMPGRAPH_DUMPENTRY"]._serialized_end = 6181
    _globals["_CACHEDUMPGRAPHMSG"]._serialized_start = 6183
    _globals["_CACHEDUMPGRAPHMSG"]._serialized_end = 6296
    _globals["_ADAPTERREGISTERED"]._serialized_start = 6298
    _globals["_ADAPTERREGISTERED"]._serialized_end = 6364
    _globals["_ADAPTERREGISTEREDMSG"]._serialized_start = 6366
    _globals["_ADAPTERREGISTEREDMSG"]._serialized_end = 6485
    _globals["_ADAPTERIMPORTERROR"]._serialized_start = 6487
    _globals["_ADAPTERIMPORTERROR"]._serialized_end = 6520
    _globals["_ADAPTERIMPORTERRORMSG"]._serialized_start = 6522
    _globals["_ADAPTERIMPORTERRORMSG"]._serialized_end = 6643
    _globals["_PLUGINLOADERROR"]._serialized_start = 6645
    _globals["_PLUGINLOADERROR"]._serialized_end = 6680
    _globals["_PLUGINLOADERRORMSG"]._serialized_start = 6682
    _globals["_PLUGINLOADERRORMSG"]._serialized_end = 6797
    _globals["_NEWCONNECTIONOPENING"]._serialized_start = 6799
    _globals["_NEWCONNECTIONOPENING"]._serialized_end = 6896
    _globals["_NEWCONNECTIONOPENINGMSG"]._serialized_start = 6898
    _globals["_NEWCONNECTIONOPENINGMSG"]._serialized_end = 7023
    _globals["_CODEEXECUTION"]._serialized_start = 7025
    _globals["_CODEEXECUTION"]._serialized_end = 7081
    _globals["_CODEEXECUTIONMSG"]._serialized_start = 7083
    _globals["_CODEEXECUTIONMSG"]._serialized_end = 7194
    _globals["_CODEEXECUTIONSTATUS"]._serialized_start = 7196
    _globals["_CODEEXECUTIONSTATUS"]._serialized_end = 7250
    _globals["_CODEEXECUTIONSTATUSMSG"]._serialized_start = 7252
    _globals["_CODEEXECUTIONSTATUSMSG"]._serialized_end = 7375
    _globals["_CATALOGGENERATIONERROR"]._serialized_start = 7377
    _globals["_CATALOGGENERATIONERROR"]._serialized_end = 7414
    _globals["_CATALOGGENERATIONERRORMSG"]._serialized_start = 7417
    _globals["_CATALOGGENERATIONERRORMSG"]._serialized_end = 7546
    _globals["_WRITECATALOGFAILURE"]._serialized_start = 7548
    _globals["_WRITECATALOGFAILURE"]._serialized_end = 7593
    _globals["_WRITECATALOGFAILUREMSG"]._serialized_start = 7595
    _globals["_WRITECATALOGFAILUREMSG"]._serialized_end = 7718
    _globals["_CATALOGWRITTEN"]._serialized_start = 7720
    _globals["_CATALOGWRITTEN"]._serialized_end = 7750
    _globals["_CATALOGWRITTENMSG"]._serialized_start = 7752
    _globals["_CATALOGWRITTENMSG"]._serialized_end = 7865
    _globals["_CANNOTGENERATEDOCS"]._serialized_start = 7867
    _globals["_CANNOTGENERATEDOCS"]._serialized_end = 7887
    _globals["_CANNOTGENERATEDOCSMSG"]._serialized_start = 7889
    _globals["_CANNOTGENERATEDOCSMSG"]._serialized_end = 8010
    _globals["_BUILDINGCATALOG"]._serialized_start = 8012
    _globals["_BUILDINGCATALOG"]._serialized_end = 8029
    _globals["_BUILDINGCATALOGMSG"]._serialized_start = 8031
    _globals["_BUILDINGCATALOGMSG"]._serialized_end = 8146
    _globals["_DATABASEERRORRUNNINGHOOK"]._serialized_start = 8148
    _globals["_DATABASEERRORRUNNINGHOOK"]._serialized_end = 8193
    _globals["_DATABASEERRORRUNNINGHOOKMSG"]._serialized_start = 8196
    _globals["_DATABASEERRORRUNNINGHOOKMSG"]._serialized_end = 8329
    _globals["_HOOKSRUNNING"]._serialized_start = 8331
    _globals["_HOOKSRUNNING"]._serialized_end = 8383
    _globals["_HOOKSRUNNINGMSG"]._serialized_start = 8385
    _globals["_HOOKSRUNNINGMSG"]._serialized_end = 8494
    _globals["_FINISHEDRUNNINGSTATS"]._serialized_start = 8496
    _globals["_FINISHEDRUNNINGSTATS"]._serialized_end = 8580
    _globals["_FINISHEDRUNNINGSTATSMSG"]._serialized_start = 8582
    _globals["_FINISHEDRUNNINGSTATSMSG"]._serialized_end = 8707
    _globals["_CONSTRAINTNOTENFORCED"]._serialized_start = 8709
    _globals["_CONSTRAINTNOTENFORCED"]._serialized_end = 8769
    _globals["_CONSTRAINTNOTENFORCEDMSG"]._serialized_start = 8771
    _globals["_CONSTRAINTNOTENFORCEDMSG"]._serialized_end = 8898
    _globals["_CONSTRAINTNOTSUPPORTED"]._serialized_start = 8900
    _globals["_CONSTRAINTNOTSUPPORTED"]._serialized_end = 8961
    _globals["_CONSTRAINTNOTSUPPORTEDMSG"]._serialized_start = 8964
    _globals["_CONSTRAINTNOTSUPPORTEDMSG"]._serialized_end = 9093
# @@protoc_insertion_point(module_scope)
