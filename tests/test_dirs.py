"""
Test directory recreation.
"""
import itertools
import io
import tarfile
import os


class Dir:
    def __init__(self, name, children):
        self.name = name
        self.children = children

    def get_children(self, parent_path):
        self_path = parent_path + self.name + "/"
        return itertools.chain.from_iterable(
            [(Dir(self_path, []),)]
            + [child.get_children(self_path) for child in self.children]
        )

    def add_to_tar(self, tar):
        ti = tarfile.TarInfo(self.name)
        ti.type = tarfile.DIRTYPE
        tar.addfile(ti)


class File:
    def __init__(self, name, size):
        self.name = name
        self.size = size

    def get_children(self, parent_path):
        return iter((File(parent_path + self.name, self.size),))

    def add_to_tar(self, tar):
        ti = tarfile.TarInfo(self.name)
        ti.type = tarfile.REGTYPE
        ti.size = self.size
        data = b"0" * self.size
        tar.addfile(ti, io.BytesIO(data))


DIRS = Dir(
    "thedir",
    [
        Dir(
            "nested1",
            [
                File("file1", 10240),
                Dir("somedir", []),
                File("file2", 10240),
            ],
        ),
        Dir(
            "nested2",
            [
                File("file1", 10240),
                File("file2", 10240),
            ],
        ),
        File("nested1/out-of-order", 1024),
        # Yep, again
        Dir("nested1/somedir", []),
    ],
)


def test_first_volume_no_create(cargo_run, tmpdir):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")

    with tarfile.open(str(inp), mode="w") as tar:
        for obj in DIRS.get_children(""):
            obj.add_to_tar(tar)
    cargo_run(["-S", "100K", "--recreate-dirs", str(inp), str(output)])
    assert os.listdir(str(outdir)) == ["output.tar.00000"]
    with tarfile.open(str(outdir.join("output.tar.00000")), "r") as tar:
        assert tar.getnames() == [
            "thedir",
            "thedir/nested1",
            "thedir/nested1/file1",
            "thedir/nested1/somedir",
            "thedir/nested1/file2",
            "thedir/nested2",
            "thedir/nested2/file1",
            "thedir/nested2/file2",
            "thedir/nested1/out-of-order",
            "thedir/nested1/somedir",
        ]


def test_next_volume_create(cargo_run, tmpdir):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")

    with tarfile.open(str(inp), mode="w") as tar:
        for obj in DIRS.get_children(""):
            obj.add_to_tar(tar)
    cargo_run(["-S", "35K", "--recreate-dirs", str(inp), str(output)])
    assert os.listdir(str(outdir)) == ["output.tar.00000", "output.tar.00001"]
    with tarfile.open(str(outdir.join("output.tar.00000")), "r") as tar:
        assert tar.getnames() == [
            "thedir",
            "thedir/nested1",
            "thedir/nested1/file1",
            "thedir/nested1/somedir",
            "thedir/nested1/file2",
            "thedir/nested2",
            "thedir/nested2/file1",
        ]
    with tarfile.open(str(outdir.join("output.tar.00001")), "r") as tar:
        assert tar.getnames() == [
            "thedir",
            "thedir/nested2",
            "thedir/nested2/file2",
            "thedir/nested1",
            "thedir/nested1/out-of-order",
            "thedir/nested1/somedir",
        ]


def test_no_create(cargo_run, tmpdir):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")

    with tarfile.open(str(inp), mode="w") as tar:
        for obj in DIRS.get_children(""):
            obj.add_to_tar(tar)
    cargo_run(["-S", "39K", str(inp), str(output)])
    assert os.listdir(str(outdir)) == ["output.tar.00000", "output.tar.00001"]
    with tarfile.open(str(outdir.join("output.tar.00000")), "r") as tar:
        assert tar.getnames() == [
            "thedir",
            "thedir/nested1",
            "thedir/nested1/file1",
            "thedir/nested1/somedir",
            "thedir/nested1/file2",
            "thedir/nested2",
            "thedir/nested2/file1",
        ]
    with tarfile.open(str(outdir.join("output.tar.00001")), "r") as tar:
        assert tar.getnames() == [
            "thedir/nested2/file2",
            "thedir/nested1/out-of-order",
            "thedir/nested1/somedir",
        ]
