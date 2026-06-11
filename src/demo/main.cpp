#include "core/yisync_protocol.hpp"
#include "core/yisync_sync.hpp"
#include "network/yisync_scheduler.hpp"
#include "receiver/yisync_receiver.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace {

yisync::Bytes bytes_from_string(std::string_view text) {
  yisync::Bytes bytes;
  bytes.reserve(text.size());
  for (char ch : text) {
    bytes.push_back(static_cast<std::byte>(static_cast<unsigned char>(ch)));
  }
  return bytes;
}

void write_text(const std::filesystem::path& path, std::string_view text) {
  std::ofstream out(path, std::ios::binary);
  out.write(text.data(), static_cast<std::streamsize>(text.size()));
}

std::string read_text(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void print_result(std::string_view op, const std::optional<yisync::T_Nack>& result) {
  if (!result.has_value()) {
    std::cout << op << " -> OK\n";
    return;
  }
  const auto& nack = *result;
  std::cout << op << " -> NACK reason=" << static_cast<int>(nack.reason)
            << " detail=" << nack.detail << "\n";
}

void print_heartbeat(const yisync::T_Heartbeat& heartbeat) {
  std::cout << "HEARTBEAT next_seq=" << heartbeat.next_seq << " file=" << heartbeat.file_id
            << " offset=" << heartbeat.offset << " durable_offset=" << heartbeat.durable_offset
            << " recv_window=" << heartbeat.recv_window_bytes << "\n";
}

yisync::T_Data make_data(std::uint64_t stream_id,
                       std::uint64_t seq,
                       std::uint64_t file_id,
                       std::uint64_t offset,
                       std::uint64_t final_size,
                       std::string_view text) {
  auto payload = bytes_from_string(text);
  return yisync::T_Data{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .offset = offset,
      .final_size = final_size,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .compression = yisync::EM_Compression::NONE,
      .checksum_algo = yisync::EM_ChecksumAlgo::CRC32C,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

yisync::T_FileChecksum full_crc32c_checksum(const yisync::Bytes& bytes) {
  return yisync::T_FileChecksum{
      .algo = yisync::EM_ChecksumAlgo::CRC32C,
      .offset = 0,
      .len = static_cast<std::uint64_t>(bytes.size()),
      .value = yisync::crc32c_bytes(bytes),
  };
}

yisync::T_Chunk make_chunk(std::uint64_t stream_id,
                         std::uint64_t seq,
                         std::uint64_t file_id,
                         std::uint64_t chunk_index,
                         std::uint64_t chunk_size,
                         const yisync::Bytes& source) {
  const auto offset = chunk_index * chunk_size;
  const auto len = std::min<std::uint64_t>(chunk_size, source.size() - offset);
  yisync::Bytes payload(source.begin() + static_cast<std::ptrdiff_t>(offset),
                        source.begin() + static_cast<std::ptrdiff_t>(offset + len));
  return yisync::T_Chunk{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .chunk_index = chunk_index,
      .offset = offset,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .compression = yisync::EM_Compression::NONE,
      .checksum_algo = yisync::EM_ChecksumAlgo::CRC32C,
      .checksum = yisync::crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

std::uint64_t encoded_message_size(const yisync::T_Message& message) {
  return yisync::encode_frame(message).size();
}

struct T_TransferLine {
  T_TransferLine(yisync::LineId line_id, std::string line_name)
      : id(line_id), name(std::move(line_name)) {}

  yisync::LineId id = 0;
  std::string name;
};

T_TransferLine& find_line(std::vector<T_TransferLine>& lines, yisync::LineId line_id) {
  auto it = std::find_if(lines.begin(), lines.end(), [line_id](const auto& line) {
    return line.id == line_id;
  });
  if (it == lines.end()) {
    throw std::runtime_error("unknown transfer line");
  }
  return *it;
}

std::vector<T_TransferLine> make_demo_lines(const std::vector<yisync::LineId>& line_ids) {
  std::vector<T_TransferLine> lines;
  lines.reserve(line_ids.size());
  for (const auto line_id : line_ids) {
    lines.emplace_back(line_id, "demo-line-" + std::to_string(line_id));
  }
  return lines;
}

std::optional<yisync::T_Nack> apply_chunk_message(const yisync::T_Message& message,
                                                yisync::T_ChunkedReceiverStream& sink) {
  if (const auto* begin = std::get_if<yisync::T_FileBegin>(&message)) {
    return sink.apply(*begin);
  }
  if (const auto* chunk = std::get_if<yisync::T_Chunk>(&message)) {
    return sink.apply(*chunk);
  }
  if (const auto* commit = std::get_if<yisync::T_FileCommit>(&message)) {
    return sink.apply(*commit);
  }
  throw std::runtime_error("message is not applicable to T_ChunkedReceiverStream");
}

void run_reconnect_demo() {
  namespace fs = std::filesystem;

  constexpr std::uint64_t stream_id = 77;
  const auto base = fs::temp_directory_path() / "yisync_reconnect_demo_cpp20";
  const auto sink_root = base / "sink";
  fs::remove_all(base);
  fs::create_directories(sink_root);

  const std::string source_data = "part-1|part-2";
  const std::string first_part = "part-1|";
  const auto file_path = sink_root / yisync::file_name_for_id(1);

  {
    yisync::T_ReceiverStream sink(stream_id, sink_root);

    yisync::T_Create create_1{
        .stream_id = stream_id,
        .seq = 1,
        .file_id = 1,
        .name = "1.file",
        .final_size = source_data.size(),
        .prev_file_id = 0,
        .prev_final_size = 0,
    };
    print_result("RECONNECT conn1 CREATE 1.file", sink.apply(create_1));

    const auto data_1 = make_data(stream_id, 1, 1, 0, source_data.size(), first_part);
    print_result("RECONNECT conn1 DATA first part", sink.apply(data_1));

    std::cout << "RECONNECT disconnect: dropped inflight DATA seq=1\n";
  }

  const auto manifest_before_resume = yisync::scan_manifest_stream(stream_id, sink_root, 4 * 1024 * 1024);
  const auto remote_offset = manifest_before_resume.entries.empty() ? 0 : manifest_before_resume.entries.back().size;
  std::cout << "RECONNECT manifest after reconnect: file=1 offset=" << remote_offset << "\n";

  {
    yisync::T_ReceiverStream sink(stream_id, sink_root);

    const auto resume_payload = source_data.substr(static_cast<std::size_t>(remote_offset));
    const auto resume_data = make_data(stream_id, 1, 1, remote_offset, source_data.size(), resume_payload);
    print_result("RECONNECT conn2 DATA resume", sink.apply(resume_data));
    print_heartbeat(sink.heartbeat(4 * 1024 * 1024, sink.state().current_offset));
  }

  const auto final_data = read_text(file_path);
  std::cout << "RECONNECT final match=" << (final_data == source_data)
            << " final_size=" << final_data.size() << "\n";
  if (final_data != source_data) {
    throw std::runtime_error("reconnect demo final file content mismatch");
  }
}

void run_scheduler_demo() {
  yisync::T_MultiLineScheduler scheduler({
      yisync::T_LineConfig{
          .id = 1,
          .name = "line-a",
          .limiter = yisync::T_TokenBucketConfig{
              .tokens_per_tick = 20 * 1024,
              .capacity = 20 * 1024,
              .tick = std::chrono::milliseconds(10),
          },
          .initial_recv_window_bytes = 64 * 1024,
      },
      yisync::T_LineConfig{
          .id = 2,
          .name = "line-b",
          .limiter = yisync::T_TokenBucketConfig{
              .tokens_per_tick = 10 * 1024,
              .capacity = 10 * 1024,
              .tick = std::chrono::milliseconds(10),
          },
          .initial_recv_window_bytes = 64 * 1024,
      },
  });

  const yisync::T_SendRequest burst{
      .stream_id = 91,
      .file_id = 1,
      .seq = 1,
      .bytes = 30 * 1024,
      .split_allowed = false,
  };
  const auto blocked = scheduler.try_acquire(burst);
  std::cout << "SCHED burst 30KB immediate grant=" << blocked.has_value() << "\n";

  const yisync::T_SendRequest chunk_1{
      .stream_id = 91,
      .file_id = 1,
      .seq = 1,
      .bytes = 20 * 1024,
      .split_allowed = false,
  };
  const yisync::T_SendRequest chunk_2{
      .stream_id = 91,
      .file_id = 1,
      .seq = 2,
      .bytes = 20 * 1024,
      .split_allowed = false,
  };
  const yisync::T_SendRequest chunk_3{
      .stream_id = 91,
      .file_id = 1,
      .seq = 3,
      .bytes = 20 * 1024,
      .split_allowed = false,
  };
  const yisync::T_SendRequest fallback_chunk{
      .stream_id = 91,
      .file_id = 1,
      .seq = 4,
      .bytes = 10 * 1024,
      .split_allowed = false,
  };
  const auto first = scheduler.try_acquire(chunk_1);
  std::cout << "SCHED tick0 20KB line=" << (first ? std::to_string(first->line_id) : "blocked") << "\n";

  const auto second = scheduler.try_acquire(chunk_2);
  std::cout << "SCHED tick0 second 20KB line=" << (second ? std::to_string(second->line_id) : "blocked") << "\n";

  scheduler.refill_ticks(1);
  const auto third = scheduler.try_acquire(chunk_2);
  std::cout << "SCHED tick1 20KB line=" << (third ? std::to_string(third->line_id) : "blocked") << "\n";

  scheduler.on_line_disconnected(1);
  const auto after_disconnect = scheduler.try_acquire(fallback_chunk);
  std::cout << "SCHED line-a disconnected, 10KB line="
            << (after_disconnect ? std::to_string(after_disconnect->line_id) : "blocked") << "\n";

  scheduler.on_line_connected(1);
  scheduler.on_heartbeat(1, yisync::T_Heartbeat{
                                 .stream_id = 91,
                                 .next_seq = 3,
                                 .file_id = 1,
                                 .offset = 20 * 1024,
                                 .durable_offset = 20 * 1024,
                                 .recv_window_bytes = 16 * 1024,
                             });
  scheduler.refill_ticks(1);
  const auto backpressured = scheduler.try_acquire(chunk_3);
  std::cout << "SCHED line-a window 16KB, 20KB grant=" << backpressured.has_value() << "\n";

  for (const auto& snapshot : scheduler.snapshots()) {
    std::cout << "SCHED snapshot line=" << snapshot.id << " tokens=" << snapshot.tokens
              << " inflight=" << snapshot.inflight_bytes
              << " window=" << snapshot.recv_window_bytes
              << " connected=" << snapshot.connected
              << " healthy=" << snapshot.healthy
              << " stale=" << snapshot.stale
              << " pending=" << snapshot.pending_sends << "\n";
  }
}

void run_chunk_transfer_demo(std::string_view label,
                             const std::filesystem::path& base,
                             std::uint64_t stream_id,
                             std::vector<T_TransferLine> lines) {
  namespace fs = std::filesystem;

  constexpr std::uint64_t seq = 1;
  constexpr std::uint64_t file_id = 1;
  const auto sink_root = base / "sink";
  fs::remove_all(base);
  fs::create_directories(sink_root);

  yisync::Bytes source_bytes;
  source_bytes.reserve(150 * 1024);
  for (std::size_t i = 0; i < 150 * 1024; ++i) {
    source_bytes.push_back(static_cast<std::byte>('A' + (i % 26)));
  }

  const auto chunk_count = yisync::chunk_count_for_size(source_bytes.size());
  std::cout << label << " use=" << yisync::should_use_chunk_mode(source_bytes.size())
            << " size=" << source_bytes.size() << " chunk_count=" << chunk_count << "\n";

  constexpr std::uint64_t line_budget = 96 * 1024;
  yisync::T_MultiLineScheduler scheduler({
      yisync::T_LineConfig{
          .id = 1,
          .name = "chunk-line-a",
          .limiter = yisync::T_TokenBucketConfig{
              .tokens_per_tick = line_budget,
              .capacity = line_budget,
              .tick = std::chrono::milliseconds(10),
          },
          .initial_recv_window_bytes = 2 * line_budget,
      },
      yisync::T_LineConfig{
          .id = 2,
          .name = "chunk-line-b",
          .limiter = yisync::T_TokenBucketConfig{
              .tokens_per_tick = line_budget,
              .capacity = line_budget,
              .tick = std::chrono::milliseconds(10),
          },
          .initial_recv_window_bytes = 2 * line_budget,
      },
  });

  if (lines.size() < 2) {
    throw std::runtime_error("chunk transfer demo requires at least two lines");
  }
  for (const auto& line : lines) {
    std::cout << label << " line ready id=" << line.id << " name=" << line.name << "\n";
  }

  const auto acquire_or_wait = [&](const yisync::T_SendRequest& request) {
    for (std::uint64_t ticks = 0; ticks < 100; ++ticks) {
      if (auto grant = scheduler.try_acquire(request)) {
        return std::pair<yisync::T_SendGrant, std::uint64_t>{*grant, ticks};
      }
      scheduler.refill_ticks(1);
    }
    throw std::runtime_error("scheduler did not grant chunk send");
  };

  const auto emit_heartbeat = [&](T_TransferLine& line, const yisync::T_Heartbeat& heartbeat) {
    const auto frame = yisync::encode_frame(yisync::T_Message{heartbeat});
    const auto decoded = yisync::decode_message(
        yisync::decode_frame(std::span<const std::byte>(frame.data(), frame.size())));
    const auto* decoded_heartbeat = std::get_if<yisync::T_Heartbeat>(&decoded);
    if (decoded_heartbeat == nullptr) {
      throw std::runtime_error("expected HEARTBEAT message");
    }
    scheduler.on_heartbeat(line.id, *decoded_heartbeat);
  };

  yisync::T_ChunkedReceiverStream sink(stream_id, sink_root);
  yisync::T_FileBegin begin{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .name = yisync::file_name_for_id(file_id),
      .final_size = static_cast<std::uint64_t>(source_bytes.size()),
      .chunk_size = yisync::kDefaultChunkSizeBytes,
      .chunk_count = chunk_count,
      .file_checksum = full_crc32c_checksum(source_bytes),
      .prev_file_id = 0,
      .prev_final_size = 0,
  };
  auto& control_line = find_line(lines, 1);
  auto begin_result = apply_chunk_message(yisync::T_Message{begin}, sink);
  print_result(std::string(label) + " FILE_BEGIN line 1", begin_result);
  if (begin_result.has_value()) {
    throw std::runtime_error("chunk FILE_BEGIN failed");
  }

  for (const auto chunk_index : std::vector<std::uint64_t>{2, 0, 1}) {
    auto chunk = make_chunk(stream_id, seq, file_id, chunk_index, yisync::kDefaultChunkSizeBytes, source_bytes);
    const auto wire_bytes = encoded_message_size(yisync::T_Message{chunk});
    const yisync::T_SendRequest request{
        .stream_id = chunk.stream_id,
        .file_id = chunk.file_id,
        .seq = chunk.seq,
        .bytes = wire_bytes,
        .split_allowed = false,
        .kind = yisync::EM_SendKind::CHUNK,
        .chunk_index = chunk.chunk_index,
    };

    const auto [grant, waited_ticks] = acquire_or_wait(request);
    auto& line = find_line(lines, grant.line_id);
    auto result = apply_chunk_message(yisync::T_Message{chunk}, sink);
    std::cout << label << " scheduled index " << chunk_index
              << " line=" << grant.line_id
              << " transport=" << line.name
              << " wire_bytes=" << wire_bytes
              << " waited_ticks=" << waited_ticks << "\n";
    print_result(std::string(label) + " DATA index " + std::to_string(chunk_index), result);
    if (result.has_value()) {
      throw std::runtime_error("chunk DATA failed");
    }

    emit_heartbeat(line,
                   yisync::T_Heartbeat{
                       .stream_id = stream_id,
                       .next_seq = sink.expected_seq(),
                       .file_id = file_id,
                       .offset = 0,
                       .durable_offset = 0,
                       .recv_window_bytes = 2 * line_budget,
                       .received_chunks = {
                           yisync::T_ReceivedChunk{
                               .seq = seq,
                               .file_id = file_id,
                               .chunk_index = chunk_index,
                           },
                       },
                   });
  }

  yisync::T_FileCommit commit{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
  };
  const auto commit_wire_bytes = encoded_message_size(yisync::T_Message{commit});
  const yisync::T_SendRequest commit_request{
      .stream_id = commit.stream_id,
      .file_id = commit.file_id,
      .seq = commit.seq,
      .bytes = commit_wire_bytes,
      .split_allowed = false,
      .kind = yisync::EM_SendKind::FILE_COMMIT,
  };
  const auto [commit_grant, commit_waited_ticks] = acquire_or_wait(commit_request);
  auto& commit_line = find_line(lines, commit_grant.line_id);
  auto commit_result = apply_chunk_message(yisync::T_Message{commit}, sink);
  std::cout << label << " FILE_COMMIT scheduled line=" << commit_grant.line_id
            << " transport=" << commit_line.name
            << " wire_bytes=" << commit_wire_bytes
            << " waited_ticks=" << commit_waited_ticks << "\n";
  print_result(std::string(label) + " FILE_COMMIT", commit_result);
  if (commit_result.has_value()) {
    throw std::runtime_error("chunk FILE_COMMIT failed");
  }
  emit_heartbeat(commit_line,
                 yisync::T_Heartbeat{
                     .stream_id = stream_id,
                     .next_seq = sink.expected_seq(),
                     .file_id = file_id,
                     .offset = static_cast<std::uint64_t>(source_bytes.size()),
                     .durable_offset = static_cast<std::uint64_t>(source_bytes.size()),
                     .recv_window_bytes = 2 * line_budget,
                 });

  for (const auto& snapshot : scheduler.snapshots()) {
    std::cout << label << " scheduler snapshot line=" << snapshot.id
              << " tokens=" << snapshot.tokens
              << " inflight=" << snapshot.inflight_bytes
              << " window=" << snapshot.recv_window_bytes << "\n";
  }

  const auto final_path = sink_root / yisync::file_name_for_id(file_id);
  const auto final_text = read_text(final_path);
  const bool final_match =
      final_text.size() == source_bytes.size() &&
      std::equal(final_text.begin(), final_text.end(), source_bytes.begin(), [](char lhs, std::byte rhs) {
        return static_cast<unsigned char>(lhs) == static_cast<unsigned char>(rhs);
      });
  std::cout << label << " final match=" << final_match << " final_size=" << final_text.size()
            << " expected_seq=" << sink.expected_seq() << "\n";

  const auto frame = yisync::encode_frame(yisync::T_Message{
      make_chunk(stream_id, seq + 1, file_id + 1, 0, yisync::kDefaultChunkSizeBytes, source_bytes)});
  const auto decoded = yisync::decode_message(yisync::decode_frame(std::span<const std::byte>(frame.data(), frame.size())));
  std::cout << "encoded " << label << " frame bytes=" << frame.size()
            << " decoded_is_chunk=" << std::holds_alternative<yisync::T_Chunk>(decoded) << "\n";

  if (!final_match) {
    throw std::runtime_error("chunk demo final file content mismatch");
  }
}

void run_chunk_no_persist_demo() {
  namespace fs = std::filesystem;

  constexpr std::uint64_t stream_id = 125;
  constexpr std::uint64_t seq = 1;
  constexpr std::uint64_t file_id = 1;
  const auto base = fs::temp_directory_path() / "yisync_chunk_recovery_demo_cpp20";
  const auto sink_root = base / "sink";
  fs::remove_all(base);
  fs::create_directories(sink_root);

  yisync::Bytes source_bytes;
  source_bytes.reserve(150 * 1024);
  for (std::size_t i = 0; i < 150 * 1024; ++i) {
    source_bytes.push_back(static_cast<std::byte>('a' + (i % 26)));
  }

  const auto chunk_count = yisync::chunk_count_for_size(source_bytes.size());
  const yisync::T_FileBegin begin{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .name = yisync::file_name_for_id(file_id),
      .final_size = static_cast<std::uint64_t>(source_bytes.size()),
      .chunk_size = yisync::kDefaultChunkSizeBytes,
      .chunk_count = chunk_count,
      .file_checksum = full_crc32c_checksum(source_bytes),
      .prev_file_id = 0,
      .prev_final_size = 0,
  };

  {
    yisync::T_ChunkedReceiverStream receiver(stream_id, sink_root);
    print_result("CHUNK-NO-PERSIST FILE_BEGIN", receiver.apply(begin));
    for (const auto chunk_index : std::vector<std::uint64_t>{2, 0}) {
      auto chunk = make_chunk(stream_id, seq, file_id, chunk_index, yisync::kDefaultChunkSizeBytes, source_bytes);
      print_result("CHUNK-NO-PERSIST partial CHUNK " + std::to_string(chunk_index), receiver.apply(chunk));
    }
    const auto missing = receiver.missing_ranges(seq, 4);
    std::cout << "CHUNK-NO-PERSIST before restart missing_ranges=" << missing.size();
    if (!missing.empty()) {
      std::cout << " first=" << missing.front().first_chunk_index
                << " last=" << missing.front().last_chunk_index;
    }
    std::cout << "\n";
  }

  const auto manifest = yisync::scan_manifest_stream(stream_id, sink_root, 4 * 1024 * 1024);
  if (!manifest.entries.empty()) {
    throw std::runtime_error("chunk no-persist manifest exposed unfinished file");
  }
  const auto manifest_frame = yisync::encode_frame(yisync::T_Message{yisync::T_Manifest1{.manifest_id = 1, .streams = {manifest}}});
  const auto manifest_decoded = yisync::decode_message(
      yisync::decode_frame(std::span<const std::byte>(manifest_frame.data(), manifest_frame.size())));
  std::cout << "CHUNK-NO-PERSIST manifest entries=" << manifest.entries.size()
            << " decoded_is_manifest=" << std::holds_alternative<yisync::T_Manifest1>(manifest_decoded) << "\n";
  const yisync::T_Manifest2 manifest2{
      .manifest_id = 1,
      .streams = {yisync::T_Manifest2Stream{
          .stream_id = stream_id,
          .action = yisync::EM_Manifest2Action::CREATE_MISSING,
          .start_file_id = file_id,
          .start_offset = 0,
      }},
  };
  const auto manifest2_frame = yisync::encode_frame(yisync::T_Message{manifest2});
  const auto manifest2_decoded = yisync::decode_message(
      yisync::decode_frame(std::span<const std::byte>(manifest2_frame.data(), manifest2_frame.size())));
  std::cout << "CHUNK-NO-PERSIST manifest2 streams=" << manifest2.streams.size()
            << " decoded_is_manifest2=" << std::holds_alternative<yisync::T_Manifest2>(manifest2_decoded) << "\n";

  {
    yisync::T_ChunkedReceiverStream receiver(stream_id, sink_root);
    std::cout << "CHUNK-NO-PERSIST restarted expected_seq=" << receiver.expected_seq() << "\n";
    print_result("CHUNK-NO-PERSIST restart FILE_BEGIN", receiver.apply(begin));
    for (std::uint64_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
      auto chunk = make_chunk(stream_id, seq, file_id, chunk_index, yisync::kDefaultChunkSizeBytes, source_bytes);
      print_result("CHUNK-NO-PERSIST restart CHUNK " + std::to_string(chunk_index), receiver.apply(chunk));
    }
    const yisync::T_FileCommit commit{
        .stream_id = stream_id,
        .seq = seq,
        .file_id = file_id,
    };
    print_result("CHUNK-NO-PERSIST FILE_COMMIT", receiver.apply(commit));
  }

  const auto final_path = sink_root / yisync::file_name_for_id(file_id);
  const auto final_text = read_text(final_path);
  const bool final_match =
      final_text.size() == source_bytes.size() &&
      std::equal(final_text.begin(), final_text.end(), source_bytes.begin(), [](char lhs, std::byte rhs) {
        return static_cast<unsigned char>(lhs) == static_cast<unsigned char>(rhs);
      });
  const auto final_manifest = yisync::scan_manifest_stream(stream_id, sink_root, 4 * 1024 * 1024);
  std::cout << "CHUNK-NO-PERSIST final match=" << final_match
            << " final_size=" << final_text.size()
            << " manifest_entries_after_commit=" << final_manifest.entries.size() << "\n";
  if (!final_match || final_manifest.entries.size() != 1) {
    throw std::runtime_error("chunk no-persist demo final state mismatch");
  }
}

void run_chunk_demo() {
  run_chunk_transfer_demo("CHUNK",
                          std::filesystem::temp_directory_path() / "yisync_chunk_demo_cpp20",
                          123,
                          make_demo_lines({1, 2}));
}

}  // namespace

int main() {
  namespace fs = std::filesystem;

  const auto base = fs::temp_directory_path() / "yisync_demo_cpp20";
  const auto source = base / "source";
  const auto sink_root = base / "sink";
  fs::remove_all(base);
  fs::create_directories(source);
  fs::create_directories(sink_root);

  write_text(source / "1.file", "hello");
  write_text(source / "2.file", "world-data");

  yisync::T_ReceiverStream sink(7, sink_root);

  yisync::T_Create create_1{
      .stream_id = 7,
      .seq = 1,
      .file_id = 1,
      .name = "1.file",
      .final_size = 5,
      .prev_file_id = 0,
      .prev_final_size = 0,
  };
  print_result("CREATE 1.file", sink.apply(create_1));

  auto payload_1 = bytes_from_string("hello");
  yisync::T_Data data_1{
      .stream_id = 7,
      .seq = 1,
      .file_id = 1,
      .offset = 0,
      .final_size = payload_1.size(),
      .raw_len = static_cast<std::uint32_t>(payload_1.size()),
      .compression = yisync::EM_Compression::NONE,
      .checksum_algo = yisync::EM_ChecksumAlgo::CRC32C,
      .checksum = yisync::crc32c_bytes(payload_1),
      .payload = payload_1,
  };
  print_result("DATA 1.file", sink.apply(data_1));

  const auto prev_checksum = yisync::make_crc32c_range_checksum(source / "1.file", 4 * 1024 * 1024);
  yisync::T_Create create_2{
      .stream_id = 7,
      .seq = 2,
      .file_id = 2,
      .name = "2.file",
      .final_size = 10,
      .prev_file_id = 1,
      .prev_final_size = 5,
      .prev_checksum = prev_checksum,
  };
  print_result("CREATE 2.file", sink.apply(create_2));

  auto payload_2 = bytes_from_string("world-data");
  yisync::T_Data data_2{
      .stream_id = 7,
      .seq = 2,
      .file_id = 2,
      .offset = 0,
      .final_size = payload_2.size(),
      .raw_len = static_cast<std::uint32_t>(payload_2.size()),
      .compression = yisync::EM_Compression::NONE,
      .checksum_algo = yisync::EM_ChecksumAlgo::CRC32C,
      .checksum = yisync::crc32c_bytes(payload_2),
      .payload = payload_2,
  };
  print_result("DATA 2.file", sink.apply(data_2));
  print_heartbeat(sink.heartbeat(4 * 1024 * 1024, sink.state().current_offset));

  const auto source_manifest = yisync::scan_manifest_stream(7, source, 4 * 1024 * 1024);
  const auto sink_manifest = yisync::scan_manifest_stream(7, sink_root, 4 * 1024 * 1024);
  const auto diff = yisync::diff_stream(7, source_manifest.entries, sink_manifest.entries);

  std::cout << "source entries=" << source_manifest.entries.size()
            << " sink entries=" << sink_manifest.entries.size() << "\n";
  std::cout << "diff result=" << (diff.has_value() ? "needs sync" : "in sync") << "\n";

  const auto frame = yisync::encode_frame(yisync::T_Message{data_2});
  const auto decoded = yisync::decode_message(yisync::decode_frame(std::span<const std::byte>(frame.data(), frame.size())));
  const auto heartbeat_frame = yisync::encode_frame(yisync::T_Message{sink.heartbeat(4 * 1024 * 1024)});
  const auto heartbeat_decoded =
      yisync::decode_message(yisync::decode_frame(std::span<const std::byte>(heartbeat_frame.data(), heartbeat_frame.size())));
  std::cout << "encoded DATA frame bytes=" << frame.size()
            << " decoded_is_data=" << std::holds_alternative<yisync::T_Data>(decoded) << "\n";
  std::cout << "encoded HEARTBEAT frame bytes=" << heartbeat_frame.size()
            << " decoded_is_heartbeat=" << std::holds_alternative<yisync::T_Heartbeat>(heartbeat_decoded) << "\n";

  run_reconnect_demo();
  run_scheduler_demo();
  run_chunk_demo();
  run_chunk_no_persist_demo();

  std::cout << "demo dir=" << base << "\n";
  return diff.has_value() ? 1 : 0;
}
