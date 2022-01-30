import io
import os
import pytest
import tarfile


def tarinfo(name, type, linkname=None, data=None):
    ti = tarfile.TarInfo(name)
    ti.type = type
    if linkname is not None:
        ti.linkname = linkname
    if data is not None:
        ti.size = len(data)
    return ti


def test_is_sane(cargo_run):
    """Check that cargo run runs, otherwise all tests will fail"""
    cargo_run(["--help"])


def test_empty(cargo_run, tmpdir):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")
    inp.write(b"\x00" * 1024)

    cargo_run(["-S", "100K", str(inp), str(output)])

    assert os.listdir(str(outdir)) == ["output.tar.00000"]
    assert outdir.join("output.tar.00000").read_binary() == (b"\x00" * 1024)


def test_suffix(cargo_run, tmpdir):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")
    inp.write(b"\x00" * 1024)

    cargo_run(["-S", "100K", "--suffix-length", "8", str(inp), str(output)])

    assert os.listdir(str(outdir)) == ["output.tar.00000000"]


@pytest.mark.parametrize(
    "tartype,linkname,contents",
    [
        (tarfile.DIRTYPE, None, None),
        (tarfile.REGTYPE, None, 1),
        (tarfile.REGTYPE, None, 1024),
        (tarfile.AREGTYPE, None, 1),
        (tarfile.AREGTYPE, None, 1024),
        (tarfile.LNKTYPE, "otherfile", None),
        (tarfile.SYMTYPE, "otherfile", None),
        (tarfile.FIFOTYPE, None, None),
    ],
)
def test_tar_type(cargo_run, tmpdir, tartype, linkname, contents):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")

    with tarfile.open(str(inp), mode="w") as tar:
        if contents is None:
            tar.addfile(tarinfo("theobject", tartype, linkname=linkname))
        else:
            data = b"1234" * contents
            tar.addfile(
                tarinfo("theobject", tartype, data=data),
                io.BytesIO(data),
            )

    cargo_run(["-S", "100K", str(inp), str(output)])
    assert os.listdir(str(outdir)) == ["output.tar.00000"]


@pytest.mark.parametrize(
    "size,expected",
    [
        (
            "40K",
            [
                "output.tar.00000",
                "output.tar.00001",
                "output.tar.00002",
                "output.tar.00003",
                "output.tar.00004",
                "output.tar.00005",
            ],
        ),
        ("80K", ["output.tar.00000", "output.tar.00001", "output.tar.00002"]),
    ],
)
def test_splits(cargo_run, tmpdir, size, expected):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")

    with tarfile.open(str(inp), mode="w") as tar:
        for i in range(10):
            data = b"1234" * (1024 * i)
            tar.addfile(
                tarinfo("theobject" + str(i), tarfile.REGTYPE, data=data),
                io.BytesIO(data),
            )

    cargo_run(["-S", size, str(inp), str(output)])
    assert sorted(os.listdir(str(outdir))) == expected


def test_file_too_large(cargo_run, tmpdir):
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")

    with tarfile.open(str(inp), mode="w") as tar:
        for i in range(10):
            data = b"1234" * (1024 * i)
            tar.addfile(
                tarinfo("theobject" + str(i), tarfile.REGTYPE, data=data),
                io.BytesIO(data),
            )

    with pytest.raises(Exception):
        cargo_run(["-S", "20K", "--fail-on-large-file", str(inp), str(output)])
    assert sorted(os.listdir(str(outdir))) == [
        "output.tar.00000",
        "output.tar.00001",
    ]


def test_verbose(cargo_run, tmpdir, monkeypatch):
    monkeypatch.setenv("TZ", "GMT-1")
    outdir = tmpdir.mkdir("out")
    output = outdir.join("output.tar.")
    inp = tmpdir.join("input.tar")

    with tarfile.open(str(inp), mode="w") as tar:
        for i, (tartype, linkname, contents) in enumerate(
            [
                (tarfile.DIRTYPE, None, None),
                (tarfile.REGTYPE, None, 1),
                (tarfile.REGTYPE, None, 1024),
                (tarfile.AREGTYPE, None, 1),
                (tarfile.AREGTYPE, None, 1024),
                (tarfile.LNKTYPE, "otherhard", None),
                (tarfile.SYMTYPE, "othersym", None),
                (tarfile.FIFOTYPE, None, None),
            ]
        ):
            if contents is None:
                tar.addfile(tarinfo("theobject" + str(i), tartype, linkname=linkname))
            else:
                data = b"1234" * contents
                tar.addfile(
                    tarinfo("theobject" + str(i), tartype, data=data),
                    io.BytesIO(data),
                )

    res = cargo_run(["-S", "10K", "-v", str(inp), str(output)])
    assert not res.stdout, repr(res.stdout)
    assert res.stderr == (
        b"""00000 drw-r--r--              0 1970-01-01 01:00:00 theobject0/
00000 -rw-r--r--              4 1970-01-01 01:00:00 theobject1
00000 -rw-r--r--           4096 1970-01-01 01:00:00 theobject2
00000 -rw-r--r--              4 1970-01-01 01:00:00 theobject3
00001 -rw-r--r--           4096 1970-01-01 01:00:00 theobject4
00001 hrw-r--r--              0 1970-01-01 01:00:00 theobject5 link to otherhard
00001 lrw-r--r--              0 1970-01-01 01:00:00 theobject6 -> othersym
00001 prw-r--r--              0 1970-01-01 01:00:00 theobject7
"""
    ), repr(res.stderr)


# TODO test for some bugs...
