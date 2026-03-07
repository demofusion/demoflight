"""Hand-coded protobuf encoding/decoding for demoflight messages.

Avoids protobuf compilation dependency by manually encoding the simple
demoflight.v1 messages. Wire format reference:
- Field header: (field_number << 3) | wire_type
- Wire type 0: varint, Wire type 2: length-delimited
"""

from __future__ import annotations

import pyarrow as pa


def encode_varint(value: int) -> bytes:
    result = []
    while value > 127:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def decode_varint(data: bytes, start: int) -> tuple[int, int]:
    result = 0
    shift = 0
    i = start
    while i < len(data):
        byte = data[i]
        result |= (byte & 0x7F) << shift
        i += 1
        if (byte & 0x80) == 0:
            break
        shift += 7
    return result, i


def encode_register_source_request(source_url: str) -> bytes:
    """
    Encode RegisterSourceRequest protobuf.

    message RegisterSourceRequest {
      string source_url = 1;
      map<string, string> options = 2;  // not used yet
    }
    """
    url_bytes = source_url.encode("utf-8")
    return b"\x0a" + encode_varint(len(url_bytes)) + url_bytes


class TableInfo:
    __slots__ = ("name", "arrow_schema")

    def __init__(self, name: str, arrow_schema: pa.Schema | None) -> None:
        self.name = name
        self.arrow_schema = arrow_schema


class RegisterSourceResponse:
    __slots__ = ("session_token", "tables")

    def __init__(self, session_token: str, tables: list[TableInfo]) -> None:
        self.session_token = session_token
        self.tables = tables


def decode_register_source_response(data: bytes) -> RegisterSourceResponse:
    """
    Decode RegisterSourceResponse protobuf.

    message RegisterSourceResponse {
      string session_token = 1;
      repeated TableInfo tables = 2;
    }

    message TableInfo {
      string name = 1;
      bytes arrow_schema = 2;
    }
    """
    session_token = ""
    tables: list[TableInfo] = []

    i = 0
    while i < len(data):
        field_byte = data[i]
        field_num = field_byte >> 3
        wire_type = field_byte & 0x07
        i += 1

        if wire_type == 2:
            length, i = decode_varint(data, i)
            value = data[i : i + length]
            i += length

            if field_num == 1:
                session_token = value.decode("utf-8")
            elif field_num == 2:
                table_info = _decode_table_info(value)
                if table_info:
                    tables.append(table_info)
        elif wire_type == 0:
            _, i = decode_varint(data, i)
        else:
            break

    return RegisterSourceResponse(session_token, tables)


def _decode_table_info(data: bytes) -> TableInfo | None:
    """Decode a TableInfo message."""
    name = ""
    arrow_schema: pa.Schema | None = None

    j = 0
    while j < len(data):
        if j >= len(data):
            break

        field_byte = data[j]
        field_num = field_byte >> 3
        wire_type = field_byte & 0x07
        j += 1

        if wire_type == 2:
            length, j = decode_varint(data, j)
            value = data[j : j + length]
            j += length

            if field_num == 1:
                name = value.decode("utf-8")
            elif field_num == 2:
                try:
                    arrow_schema = pa.ipc.read_schema(pa.BufferReader(value))
                except Exception:
                    pass
        elif wire_type == 0:
            _, j = decode_varint(data, j)
        else:
            break

    if name:
        return TableInfo(name, arrow_schema)
    return None


def encode_command_statement_query(query: str) -> bytes:
    """
    Encode a Flight SQL CommandStatementQuery wrapped in google.protobuf.Any.

    message CommandStatementQuery {
      string query = 1;
      string transaction_id = 2;  // not used
    }
    """
    query_bytes = query.encode("utf-8")
    command_bytes = b"\x0a" + encode_varint(len(query_bytes)) + query_bytes

    type_url = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    type_url_field = b"\x0a" + encode_varint(len(type_url)) + type_url
    value_field = b"\x12" + encode_varint(len(command_bytes)) + command_bytes

    return type_url_field + value_field


def encode_close_session_request(session_token: str) -> bytes:
    """
    Encode CloseSessionRequest protobuf.

    message CloseSessionRequest {
      string session_token = 1;
    }
    """
    token_bytes = session_token.encode("utf-8")
    return b"\x0a" + encode_varint(len(token_bytes)) + token_bytes
