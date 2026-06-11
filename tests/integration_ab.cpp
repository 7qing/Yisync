#!/usr/bin/env python3
import argparse
import hashlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def write_bytes(path: Path, size: int, seed: int = 1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = bytearray(size)
    for i in range(size):
        data[i] = (seed + i * 31) % 251
    path.write_bytes(data)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def run_cmd(cmd, timeout=30):
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise AssertionError(
            "command failed\ncmd={}\ncode={}\nstdout={}\nstderr={}".format(
                cmd, proc.returncode, proc.stdout, proc.stderr
            )
        )
    return proc.stdout + proc.stderr


def run_cmd_expect_fail(cmd, timeout=30):
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if proc.returncode == 0:
        raise AssertionError(
            "command unexpectedly succeeded\ncmd={}\nstdout={}\nstderr={}".format(
                cmd, proc.stdout, proc.stderr
            )
        )
    return proc.stdout + proc.stderr


def wait_for_listen(proc, needle="line listening", count=1, timeout=5):
    deadline = time.time() + timeout
    output = []
    seen = 0
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line:
            output.append(line)
            if needle in line:
                seen += 1
                if seen >= count:
                    return "".join(output)
        elif proc.poll() is not None:
            break
        else:
            time.sleep(0.02)
    raise AssertionError("receiver did not start\noutput={}".format("".join(output)))


def collect_process(proc, timeout=20):
    try:
        out, _ = proc.communicate(timeout=timeout)
        return proc.returncode, out
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            out, _ = proc.communicate(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, _ = proc.communicate(timeout=2)
        raise AssertionError("process timeout\noutput={}".format(out))


def start_receiver(node: Path, root: Path, base_port: int, lines: int = 2, config: Path | None = None):
    if config is None:
        cmd = [
            str(node),
            "receiver",
            "--host",
            "127.0.0.1",
            "--base-port",
            str(base_port),
            "--lines",
            str(lines),
            "--root",
            str(root),
        ]
    else:
        cmd = [str(node), "receiver", "--config", str(config)]
    proc = subprocess.Popen(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    wait_for_listen(proc, count=lines)
    return proc


def run_sender(node: Path,
               source: Path,
               base_port: int,
               lines: int = 2,
               drop_line_once: int | None = None,
               config: Path | None = None,
               expect_fail: bool = False):
    if config is None:
        cmd = [
            str(node),
            "sender",
            "--host",
            "127.0.0.1",
            "--base-port",
            str(base_port),
            "--lines",
            str(lines),
            "--source-root",
            str(source),
        ]
    else:
        cmd = [str(node), "sender", "--config", str(config)]
    if drop_line_once is not None:
        cmd += ["--drop-line-once", str(drop_line_once)]
    if expect_fail:
        return run_cmd_expect_fail(cmd, timeout=40)
    return run_cmd(cmd, timeout=40)


def run_fault_client(client: Path, port: int, scenario: str):
    return run_cmd([str(client), "127.0.0.1", str(port), scenario], timeout=20)


def stop_receiver(proc, timeout=20):
    code, out = collect_process(proc, timeout=timeout)
    if code != 0:
        raise AssertionError("receiver failed code={}\noutput={}".format(code, out))
    return out


def terminate_receiver(proc):
    if proc.poll() is None:
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        out, _ = proc.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        out, _ = proc.communicate(timeout=3)
    return out


def run_pair(node: Path,
             source: Path,
             receiver: Path,
             port: int,
             lines: int = 2,
             config: Path | None = None,
             drop_line_once: int | None = None):
    recv = start_receiver(node, receiver, port, lines, config)
    try:
        sender_out = run_sender(node, source, port, lines, drop_line_once, config)
        recv_out = stop_receiver(recv)
        recv = None
        return sender_out, recv_out
    finally:
        if recv is not None and recv.poll() is None:
            terminate_receiver(recv)


def receiver_path(receiver_root: Path, source: Path, relative: str) -> Path:
    return receiver_root / source.relative_to(source.anchor) / relative


def assert_same(src: Path, dst: Path):
    if not dst.exists():
        raise AssertionError("missing destination: {}".format(dst))
    if sha256(src) != sha256(dst):
        raise AssertionError("content mismatch\nsrc={}\ndst={}".format(src, dst))


def write_config(path: Path,
                 source: Path,
                 receiver: Path,
                 port: int,
                 high_dirs: str,
                 link_num: int = 2,
                 bandwidth: str = "{96KB,96KB}",
                 extra_common: str = ""):
    path.write_text(
        f"""[common]
link_num={link_num}
Bandwidth_Limit={bandwidth}
recv_window=192KB
chunk_size=64KB
heartbeat_interval_ms=50
heartbeat_ack_batch_size=20
heartbeat_timeout_ticks=300
chunk_retransmit_ticks=100
max_retransmit_retries=5
max_manifest_recovery_attempts=3
max_missing_ranges=64
reconnect_base_delay_ms=100
reconnect_max_delay_ms=2000
watch=false
compress=none
checksum=crc32c
{extra_common}
[sender]
ip=127.0.0.1
port={port}
high_priority_dirs={high_dirs}
low_priority_dirs={{}}
[receiver]
ip=127.0.0.1
port={port}
mount_dir={receiver}
""",
        encoding="utf-8",
    )


def scenario_basic(node: Path, tmp: Path, port: int):
    source = tmp / "source"
    receiver = tmp / "receiver"
    write_bytes(source / "big.bin", 150 * 1024, 3)
    (source / "sub").mkdir(parents=True)
    (source / "sub" / "small.txt").write_text("basic\n", encoding="utf-8")

    sender_out, recv_out = run_pair(node, source, receiver, port, 2)
    assert "hello_sent" in sender_out
    assert "negotiated" in sender_out
    assert "SENDER metrics" in sender_out
    assert "FILE_COMMIT complete" in recv_out
    assert "RECEIVER metrics" in recv_out
    assert_same(source / "big.bin", receiver_path(receiver, source, "big.bin"))
    assert_same(source / "sub" / "small.txt", receiver_path(receiver, source, "sub/small.txt"))


def scenario_reconnect(node: Path, tmp: Path, port: int):
    source = tmp / "source"
    receiver = tmp / "receiver"
    write_bytes(source / "drop.bin", 220 * 1024, 5)
    sender_out, recv_out = run_pair(node, source, receiver, port, 2, drop_line_once=2)
    assert "line_lost" in sender_out
    assert sender_out.count("hello_sent") >= 3
    assert "FILE_COMMIT complete" in recv_out
    assert_same(source / "drop.bin", receiver_path(receiver, source, "drop.bin"))


def scenario_receiver_restart(node: Path, tmp: Path, port: int):
    source = tmp / "source"
    receiver = tmp / "receiver"
    write_bytes(source / "restart.bin", 160 * 1024, 7)

    old_tmp = receiver / source.relative_to(source.anchor) / ".yisync_tmp"
    old_tmp.mkdir(parents=True)
    (old_tmp / "stale.tmp").write_bytes(b"stale")

    recv = start_receiver(node, receiver, port, 2)
    terminate_receiver(recv)
    recv = start_receiver(node, receiver, port, 2)
    try:
        sender_out = run_sender(node, source, port, 2)
        recv_out = stop_receiver(recv)
        recv = None
    finally:
        if recv is not None and recv.poll() is None:
            terminate_receiver(recv)
    assert "FILE_BEGIN" in sender_out
    assert "FILE_COMMIT complete" in recv_out
    assert not (old_tmp / "stale.tmp").exists()
    assert_same(source / "restart.bin", receiver_path(receiver, source, "restart.bin"))


def scenario_entries(node: Path, tmp: Path, port: int):
    source = tmp / "source"
    receiver = tmp / "receiver"
    (source / "empty").mkdir(parents=True)
    (source / "dir").mkdir(parents=True)
    (source / "dir" / "file.txt").write_text("entry\n", encoding="utf-8")
    symlink_path = source / "link.txt"
    try:
        symlink_path.symlink_to("dir/file.txt")
    except (OSError, NotImplementedError):
        symlink_path.write_text("symlink-fallback\n", encoding="utf-8")

    run_pair(node, source, receiver, port, 2)
    target_root = receiver / source.relative_to(source.anchor)
    assert (target_root / "empty").is_dir()
    assert_same(source / "dir" / "file.txt", target_root / "dir" / "file.txt")
    if symlink_path.is_symlink():
        assert (target_root / "link.txt").is_symlink()
        assert os.readlink(target_root / "link.txt") == "dir/file.txt"


def scenario_multistream(node: Path, tmp: Path, port: int):
    a = tmp / "multi_a"
    b = tmp / "multi_b"
    receiver = tmp / "receiver"
    write_bytes(a / "a.file", 96 * 1024, 11)
    write_bytes(b / "b.dat", 80 * 1024, 13)
    (a / "skip.tmp").write_text("skip\n", encoding="utf-8")
    config = tmp / "config.txt"
    high_dirs = "{" + f"{a}::.*\\.(file|bin)$,{b}::.*\\.(keep|dat)$" + "}"
    write_config(config, a, receiver, port, high_dirs)

    run_pair(node, a, receiver, port, 2, config=config)
    assert_same(a / "a.file", receiver / a.relative_to(a.anchor) / "a.file")
    assert_same(b / "b.dat", receiver / b.relative_to(b.anchor) / "b.dat")
    assert not (receiver / a.relative_to(a.anchor) / "skip.tmp").exists()


def scenario_limit(node: Path, tmp: Path, port: int):
    source = tmp / "source"
    receiver = tmp / "receiver"
    write_bytes(source / "limited.bin", 128 * 1024, 17)
    config = tmp / "config.txt"
    write_config(config,
                 source,
                 receiver,
                 port,
                 "{" + f"{source}::.*" + "}",
                 bandwidth="{80KB,80KB}")
    sender_out, _ = run_pair(node, source, receiver, port, 2, config=config)
    assert "send CHUNK" in sender_out
    assert_same(source / "limited.bin", receiver / source.relative_to(source.anchor) / "limited.bin")


def scenario_recovery(node: Path, tmp: Path, port: int):
    source = tmp / "source"
    receiver = tmp / "receiver"
    write_bytes(source / "recover.bin", 128 * 1024, 19)
    config = tmp / "config.txt"
    write_config(config,
                 source,
                 receiver,
                 port,
                 "{" + f"{source}::.*" + "}",
                 extra_common="max_retransmit_retries=0\n")
    sender_out, _ = run_pair(node, source, receiver, port, 2, config=config, drop_line_once=2)
    assert "recovery manifest1" in sender_out
    assert_same(source / "recover.bin", receiver / source.relative_to(source.anchor) / "recover.bin")


def scenario_final_failure(node: Path, tmp: Path, port: int):
    source = tmp / "source"
    receiver = tmp / "receiver"
    write_bytes(source / "fail.bin", 128 * 1024, 23)
    config = tmp / "config.txt"
    write_config(config,
                 source,
                 receiver,
                 port,
                 "{" + f"{source}::.*" + "}",
                 extra_common="chunk_retransmit_ticks=1\nmax_retransmit_retries=0\nmax_manifest_recovery_attempts=0\n")
    recv = start_receiver(node, receiver, port, 2, config)
    try:
        sender_out = run_sender(node, source, port, 2, drop_line_once=2, config=config, expect_fail=True)
    finally:
        if recv.poll() is None:
            terminate_receiver(recv)
    assert "SENDER final failure" in sender_out


def scenario_faults(node: Path, fault_client: Path, tmp: Path, port: int):
    for index, scenario in enumerate(("bad-checksum", "bad-commit", "size-conflict")):
        receiver = tmp / f"receiver_{scenario}"
        recv = start_receiver(node, receiver, port + index * 2, 1)
        try:
            run_fault_client(fault_client, port + index * 2, scenario)
        finally:
            if recv.poll() is None:
                terminate_receiver(recv)


SCENARIOS = {
    "basic": scenario_basic,
    "reconnect": scenario_reconnect,
    "receiver-restart": scenario_receiver_restart,
    "entries": scenario_entries,
    "multistream": scenario_multistream,
    "limit": scenario_limit,
    "recovery": scenario_recovery,
    "final-failure": scenario_final_failure,
    "faults": scenario_faults,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node", required=True, type=Path)
    parser.add_argument("--fault-client", type=Path)
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    args = parser.parse_args()

    scenario_ports = {
        "basic": 21010,
        "reconnect": 21030,
        "receiver-restart": 21050,
        "entries": 21070,
        "multistream": 21090,
        "limit": 21110,
        "recovery": 21130,
        "final-failure": 21150,
        "faults": 21170,
    }
    base_port = scenario_ports[args.scenario]
    tmp = Path(tempfile.mkdtemp(prefix=f"yisync_{args.scenario}_", dir="/private/tmp"))
    try:
        if args.scenario == "faults":
            if args.fault_client is None:
                raise AssertionError("--fault-client is required for faults scenario")
            SCENARIOS[args.scenario](args.node, args.fault_client, tmp, base_port)
        else:
            SCENARIOS[args.scenario](args.node, tmp, base_port)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"integration scenario failed: {exc}", file=sys.stderr)
        raise
