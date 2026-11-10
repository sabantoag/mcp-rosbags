#!/usr/bin/env python3
"""
Format-agnostic bag access for ROS 1 (.bag) and ROS 2 (rosbag2 directories,
.mcap, .db3).

`open_bag(path)` returns a reader that looks like the rosbags rosbag2 `Reader`
(`with ... as reader`, `reader.connections`, `reader.messages()`), so the rest
of the server keeps its existing loops. Messages come back as `RawMessage`
objects (bytes) that remember which reader can decode them, which lets
`deserialize()` pick the right decoder (ROS 1 serialization or CDR) and use the
message definitions stored inside the bag. That is what makes custom message
types (for example `faas_msgs/*`) readable.
"""

from pathlib import Path
from typing import Any, Iterable, List, Optional
import logging
import warnings

from rosbags.highlevel import AnyReader
from rosbags.rosbag2 import Reader as Ros2Reader

logger = logging.getLogger(__name__)

ROS1_SUFFIX = ".bag"
ROS2_SUFFIXES = (".mcap", ".db3")
BAG_SUFFIXES = (ROS1_SUFFIX,) + ROS2_SUFFIXES


class RawMessage(bytes):
    """Raw serialized message that knows which reader can deserialize it."""

    _reader: Any = None


def is_ros2_bag_dir(path: Path) -> bool:
    """A rosbag2 recording is a directory with a metadata.yaml."""
    return path.is_dir() and (path / "metadata.yaml").exists()


def is_bag_path(path: Path) -> bool:
    """True if path is a single bag file or a ROS 2 bag directory."""
    if path.is_file():
        return path.suffix in BAG_SUFFIXES
    return is_ros2_bag_dir(path)


def find_bags(search_path: Path, max_depth: int = 3) -> List[Path]:
    """
    Find ROS 1 .bag files and ROS 2 bags (directories with metadata.yaml, or
    loose .mcap/.db3 files) at or under search_path, up to max_depth levels.

    Typical layouts handled:
      - a single bag file
      - a ROS 2 bag directory
      - a folder of bags
      - a downloaded mission folder: <mission>/bag_files/*.bag
    """
    if search_path.is_file():
        return [search_path] if search_path.suffix in BAG_SUFFIXES else []
    if not search_path.is_dir():
        return []
    if is_ros2_bag_dir(search_path):
        return [search_path]

    found: List[Path] = []

    def walk(directory: Path, depth: int) -> None:
        try:
            entries = sorted(directory.iterdir())
        except OSError as e:
            logger.debug(f"Cannot list {directory}: {e}")
            return
        for item in entries:
            if item.name.startswith("."):
                continue
            if item.is_file() and item.suffix in BAG_SUFFIXES:
                found.append(item)
            elif item.is_dir():
                if is_ros2_bag_dir(item):
                    found.append(item)
                elif depth < max_depth:
                    walk(item, depth + 1)

    walk(search_path, 1)
    return sorted(found)


def bag_size_bytes(path: Path) -> int:
    """Total size of a bag file or ROS 2 bag directory."""
    if path.is_file():
        return path.stat().st_size
    return sum(
        f.stat().st_size
        for f in path.iterdir()
        if f.is_file() and f.suffix in ROS2_SUFFIXES
    )


def bag_kind(path: Path) -> str:
    """'ros1', 'ros2' or 'unknown'."""
    if path.suffix == ROS1_SUFFIX:
        return "ros1"
    if is_ros2_bag_dir(path) or path.suffix in ROS2_SUFFIXES:
        return "ros2"
    return "unknown"


def normalize_msgtype(msgtype: str) -> str:
    """
    Return the ROS 2 style name 'pkg/msg/Name' for a ROS 1 name 'pkg/Name'.
    Used only for schema and config lookups. Decoding uses the original name.
    """
    parts = msgtype.replace(".", "/").split("/")
    if len(parts) == 2:
        return f"{parts[0]}/msg/{parts[1]}"
    return "/".join(parts)


class BagReader:
    """Wraps AnyReader (preferred) or the plain rosbag2 Reader (fallback)."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._any: Optional[AnyReader] = None
        self._ros2: Optional[Ros2Reader] = None

    # -- context manager -------------------------------------------------
    def __enter__(self) -> "BagReader":
        self.open()
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    def open(self) -> None:
        kind = bag_kind(self.path)
        if kind == "ros1" or is_ros2_bag_dir(self.path):
            # AnyReader registers message definitions stored in the bag, so
            # custom types decode. It reads ROS 1 files and rosbag2 directories.
            reader = AnyReader([self.path])
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                reader.open()
            self._any = reader
        else:
            # Loose .mcap/.db3 file: no metadata.yaml, use the plain reader.
            self._ros2 = Ros2Reader(self.path)
            self._ros2.open()

    def close(self) -> None:
        if self._any is not None:
            self._any.close()
            self._any = None
        if self._ros2 is not None:
            self._ros2.close()
            self._ros2 = None

    # -- reader-like API ---------------------------------------------------
    @property
    def _reader(self):
        return self._any if self._any is not None else self._ros2

    @property
    def connections(self):
        return self._reader.connections

    @property
    def compression_mode(self) -> Optional[str]:
        return getattr(self._reader, "compression_mode", None)

    @property
    def kind(self) -> str:
        return bag_kind(self.path)

    def messages(self, connections: Optional[Iterable] = None, **kwargs):
        """Yield (connection, timestamp_ns, RawMessage)."""
        if connections is not None:
            kwargs["connections"] = list(connections)
        for conn, timestamp, rawdata in self._reader.messages(**kwargs):
            raw = RawMessage(rawdata)
            raw._reader = self
            yield conn, timestamp, raw

    def deserialize(self, rawdata: bytes, msgtype: str):
        if self._any is not None:
            return self._any.deserialize(bytes(rawdata), msgtype)
        from rosbags.serde import deserialize_cdr

        return deserialize_cdr(bytes(rawdata), msgtype)


def open_bag(path) -> BagReader:
    """Open a ROS 1 or ROS 2 bag. Use as a context manager."""
    return BagReader(Path(path))


def deserialize(rawdata: bytes, msgtype: str):
    """Decode a message that came from BagReader.messages()."""
    reader = getattr(rawdata, "_reader", None)
    if reader is None:
        from rosbags.serde import deserialize_cdr

        return deserialize_cdr(rawdata, msgtype)
    return reader.deserialize(rawdata, msgtype)
