"""Appends DuckDB's extension footer to a shared library.

usage: append_metadata.py LIBRARY OUTPUT PLATFORM DUCKDB_VERSION [EXTENSION_VERSION]

The footer is a WebAssembly-style custom section named "duckdb_signature" holding eight
32-byte fields (last to first) and 256 bytes for a signature, which unsigned extensions
leave empty.
"""

import shutil
import sys


def field(value: str) -> bytes:
    data = value.encode("ascii")
    if len(data) > 32:
        raise ValueError(f"metadata field too long: {value}")
    return data + b"\x00" * (32 - len(data))


def main() -> None:
    library, output, platform, duckdb_version = sys.argv[1:5]
    extension_version = sys.argv[5] if len(sys.argv) > 5 else "0.1.0"
    # custom section id 0, LEB128 size 531, 16-byte name, LEB128 payload size 512
    header = bytes([0, 0x93, 0x04, 16]) + b"duckdb_signature" + bytes([0x80, 0x04])
    fields = ["", "", "", "CPP", extension_version, duckdb_version, platform, "4"]
    shutil.copyfile(library, output)
    with open(output, "ab") as f:
        f.write(header)
        for value in fields:
            f.write(field(value))
        f.write(b"\x00" * 256)


if __name__ == "__main__":
    main()
