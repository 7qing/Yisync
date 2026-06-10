#include "node/yisync_node_common.hpp"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

namespace yisync::node {

Bytes make_sender_bytes(std::uint64_t size) {
  Bytes bytes;
  bytes.reserve(static_cast<std::size_t>(size));
  for (std::uint64_t i = 0; i < size; ++i) {
    bytes.push_back(static_cast<std::byte>('A' + (i % 26)));
  }
  return bytes;
}

std::uint64_t local_file_size_or_zero(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  return ec ? 0 : size;
}

void fsync_file_for_durable_offset(const std::filesystem::path& path) {
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    throw std::runtime_error("failed to open append file for fsync: " + path.string());
  }
  if (::fsync(fd) != 0) {
    const auto error = errno;
    ::close(fd);
    throw std::runtime_error("failed to fsync append file: " + std::string(std::strerror(error)));
  }
  ::close(fd);
}

FileChecksum full_crc32c_checksum(const Bytes& bytes) {
  return FileChecksum{
      .algo = ChecksumAlgo::Crc32c,
      .offset = 0,
      .len = static_cast<std::uint64_t>(bytes.size()),
      .value = crc32c_bytes(bytes),
  };
}

Chunk make_chunk_from_payload(std::uint64_t stream_id,
                              std::uint64_t seq,
                              std::uint64_t file_id,
                              std::uint64_t chunk_index,
                              Bytes payload,
                              std::uint64_t chunk_size) {
  const auto offset = chunk_index * chunk_size;
  return Chunk{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .chunk_index = chunk_index,
      .offset = offset,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .compression = Compression::None,
      .checksum_algo = ChecksumAlgo::Crc32c,
      .checksum = crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

Data make_data_from_payload(std::uint64_t stream_id,
                            std::uint64_t file_id,
                            std::uint64_t seq,
                            std::uint64_t offset,
                            std::uint64_t final_size,
                            Bytes payload) {
  return Data{
      .stream_id = stream_id,
      .seq = seq,
      .file_id = file_id,
      .offset = offset,
      .final_size = final_size,
      .raw_len = static_cast<std::uint32_t>(payload.size()),
      .compression = Compression::None,
      .checksum_algo = ChecksumAlgo::Crc32c,
      .checksum = crc32c_bytes(payload),
      .payload = std::move(payload),
  };
}

std::uint64_t encoded_message_size(const Message& message) {
  return encode_frame(message).size();
}

std::vector<std::uint64_t> chunk_send_order(std::uint64_t chunk_count) {
  std::vector<std::uint64_t> order;
  if (chunk_count == 0) {
    return order;
  }
  order.push_back(chunk_count - 1);
  for (std::uint64_t i = 0; i + 1 < chunk_count; ++i) {
    order.push_back(i);
  }
  return order;
}

std::optional<std::uint64_t> parse_u64_text(std::string_view text) {
  std::uint64_t value = 0;
  const auto* begin = text.data();
  const auto* end = text.data() + text.size();
  const auto result = std::from_chars(begin, end, value);
  if (result.ec != std::errc{} || result.ptr != end) {
    return std::nullopt;
  }
  return value;
}

std::optional<std::uint64_t> parse_stream_dir_name(const std::filesystem::path& path) {
  return parse_u64_text(path.filename().string());
}

std::uint16_t line_port(const NodeOptions& options, LineId line_id) {
  return static_cast<std::uint16_t>(options.base_port + line_id - 1);
}

namespace {

std::string trim(std::string_view text) {
  std::size_t begin = 0;
  while (begin < text.size() &&
         std::isspace(static_cast<unsigned char>(text[begin])) != 0) {
    ++begin;
  }
  std::size_t end = text.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(text[end - 1])) != 0) {
    --end;
  }
  return std::string(text.substr(begin, end - begin));
}

std::string lower_ascii(std::string_view text) {
  std::string out;
  out.reserve(text.size());
  for (const char ch : text) {
    out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
  }
  return out;
}

std::string strip_braces(std::string value) {
  value = trim(value);
  if (value.size() >= 2 && value.front() == '{' && value.back() == '}') {
    return trim(std::string_view(value).substr(1, value.size() - 2));
  }
  return value;
}

std::string strip_inline_comment(std::string_view line) {
  bool in_quote = false;
  char quote = '\0';
  for (std::size_t i = 0; i < line.size(); ++i) {
    const char ch = line[i];
    if ((ch == '"' || ch == '\'') && (i == 0 || line[i - 1] != '\\')) {
      if (!in_quote) {
        in_quote = true;
        quote = ch;
      } else if (quote == ch) {
        in_quote = false;
      }
      continue;
    }
    if (!in_quote && (ch == '#' || ch == ';')) {
      return trim(line.substr(0, i));
    }
  }
  return trim(line);
}

std::vector<std::string> split_list(std::string_view value) {
  std::vector<std::string> items;
  const auto unwrapped = strip_braces(std::string(value));
  std::size_t begin = 0;
  while (begin <= unwrapped.size()) {
    const auto comma = unwrapped.find(',', begin);
    const auto end = comma == std::string::npos ? unwrapped.size() : comma;
    auto item = trim(std::string_view(unwrapped).substr(begin, end - begin));
    if (!item.empty()) {
      if (item.size() >= 2 &&
          ((item.front() == '"' && item.back() == '"') ||
           (item.front() == '\'' && item.back() == '\''))) {
        item = item.substr(1, item.size() - 2);
      }
      if (!item.empty()) {
        items.push_back(std::move(item));
      }
    }
    if (comma == std::string::npos) {
      break;
    }
    begin = comma + 1;
  }
  return items;
}

std::optional<std::uint64_t> parse_u64_strict(std::string_view text) {
  const auto trimmed = trim(text);
  if (trimmed.empty()) {
    return std::nullopt;
  }
  std::uint64_t value = 0;
  const auto* begin = trimmed.data();
  const auto* end = trimmed.data() + trimmed.size();
  const auto result = std::from_chars(begin, end, value);
  if (result.ec != std::errc{} || result.ptr != end) {
    return std::nullopt;
  }
  return value;
}

std::optional<std::uint64_t> parse_size_bytes(std::string_view text) {
  auto value = trim(text);
  if (value.empty()) {
    return std::nullopt;
  }
  value.erase(std::remove(value.begin(), value.end(), '_'), value.end());

  std::size_t number_end = 0;
  while (number_end < value.size() &&
         std::isdigit(static_cast<unsigned char>(value[number_end])) != 0) {
    ++number_end;
  }
  if (number_end == 0) {
    return std::nullopt;
  }

  const auto number = parse_u64_strict(std::string_view(value).substr(0, number_end));
  if (!number.has_value()) {
    return std::nullopt;
  }
  auto suffix = lower_ascii(trim(std::string_view(value).substr(number_end)));
  if (!suffix.empty() && suffix.back() == 'b') {
    suffix.pop_back();
  }

  std::uint64_t multiplier = 1;
  if (suffix.empty()) {
    multiplier = 1;
  } else if (suffix == "k" || suffix == "kb" || suffix == "ki" || suffix == "kib") {
    multiplier = 1024;
  } else if (suffix == "m" || suffix == "mb" || suffix == "mi" || suffix == "mib") {
    multiplier = 1024 * 1024;
  } else if (suffix == "g" || suffix == "gb" || suffix == "gi" || suffix == "gib") {
    multiplier = 1024ULL * 1024ULL * 1024ULL;
  } else {
    return std::nullopt;
  }

  if (*number > std::numeric_limits<std::uint64_t>::max() / multiplier) {
    return std::nullopt;
  }
  return *number * multiplier;
}

std::uint16_t checked_port(std::uint64_t value, std::string_view key) {
  if (value == 0 || value > 65535) {
    throw std::runtime_error(std::string(key) + " must be in range 1..65535");
  }
  return static_cast<std::uint16_t>(value);
}

Compression parse_compression(std::string_view value) {
  const auto text = lower_ascii(trim(value));
  if (text.empty() || text == "none" || text == "off" || text == "false" || text == "0") {
    return Compression::None;
  }
  if (text == "lz4") {
    return Compression::Lz4;
  }
  if (text == "zstd") {
    return Compression::Zstd;
  }
  throw std::runtime_error("unsupported compression in config: " + std::string(value));
}

ChecksumAlgo parse_checksum(std::string_view value) {
  const auto text = lower_ascii(trim(value));
  if (text.empty() || text == "crc32c" || text == "crc") {
    return ChecksumAlgo::Crc32c;
  }
  if (text == "none" || text == "off" || text == "false" || text == "0") {
    return ChecksumAlgo::None;
  }
  if (text == "md5") {
    return ChecksumAlgo::Md5;
  }
  throw std::runtime_error("unsupported checksum in config: " + std::string(value));
}

bool parse_bool(std::string_view value) {
  const auto text = lower_ascii(trim(value));
  if (text == "1" || text == "true" || text == "yes" || text == "on") {
    return true;
  }
  if (text == "0" || text == "false" || text == "no" || text == "off") {
    return false;
  }
  throw std::runtime_error("bad boolean value in config: " + std::string(value));
}

WatchBackend parse_watch_backend(std::string_view value) {
  const auto text = lower_ascii(trim(value));
  if (text.empty() || text == "auto") {
    return WatchBackend::Auto;
  }
  if (text == "polling" || text == "poll") {
    return WatchBackend::Polling;
  }
  if (text == "inotify") {
    return WatchBackend::Inotify;
  }
  if (text == "fsevents" || text == "fsevent") {
    return WatchBackend::Fsevents;
  }
  throw std::runtime_error("unsupported watch backend in config: " + std::string(value));
}

struct IniConfig {
  std::unordered_map<std::string, std::unordered_map<std::string, std::string>> values;

  std::optional<std::string> get(std::string_view section, std::string_view key) const {
    const auto section_it = values.find(lower_ascii(section));
    if (section_it == values.end()) {
      return std::nullopt;
    }
    const auto key_it = section_it->second.find(lower_ascii(key));
    if (key_it == section_it->second.end()) {
      return std::nullopt;
    }
    return key_it->second;
  }

  std::optional<std::string> get_any(std::string_view section,
                                     std::initializer_list<std::string_view> keys) const {
    for (const auto key : keys) {
      if (auto value = get(section, key)) {
        return value;
      }
    }
    return std::nullopt;
  }
};

IniConfig load_ini_config(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to open config: " + path.string());
  }

  IniConfig config;
  std::string section;
  std::string line;
  std::uint64_t line_no = 0;
  while (std::getline(input, line)) {
    ++line_no;
    auto text = strip_inline_comment(line);
    if (text.empty()) {
      continue;
    }
    if (text.front() == '[' && text.back() == ']') {
      section = lower_ascii(trim(std::string_view(text).substr(1, text.size() - 2)));
      if (section.empty()) {
        throw std::runtime_error("empty config section at line " + std::to_string(line_no));
      }
      continue;
    }
    const auto equal = text.find('=');
    if (equal == std::string::npos) {
      throw std::runtime_error("bad config line " + std::to_string(line_no) + ": " + text);
    }
    if (section.empty()) {
      throw std::runtime_error("config key before section at line " + std::to_string(line_no));
    }
    auto key = lower_ascii(trim(std::string_view(text).substr(0, equal)));
    auto value = trim(std::string_view(text).substr(equal + 1));
    if (key.empty()) {
      throw std::runtime_error("empty config key at line " + std::to_string(line_no));
    }
    config.values[section][key] = std::move(value);
  }
  return config;
}

std::vector<std::uint16_t> parse_ports(const IniConfig& config,
                                       std::string_view mode,
                                       std::uint16_t fallback_base_port) {
  if (auto ports_text = config.get_any("common", {"ports", "line_ports"})) {
    const auto parts = split_list(*ports_text);
    std::vector<std::uint16_t> ports;
    ports.reserve(parts.size());
    for (const auto& part : parts) {
      const auto value = parse_u64_strict(part);
      if (!value.has_value()) {
        throw std::runtime_error("bad port value in config: " + part);
      }
      ports.push_back(checked_port(*value, "port"));
    }
    return ports;
  }

  auto port_value = config.get_any(mode, {"port"});
  if (port_value.has_value() && !trim(*port_value).empty()) {
    const auto parts = split_list(*port_value);
    if (parts.size() > 1) {
      std::vector<std::uint16_t> ports;
      ports.reserve(parts.size());
      for (const auto& part : parts) {
        const auto value = parse_u64_strict(part);
        if (!value.has_value()) {
          throw std::runtime_error("bad port value in config: " + part);
        }
        ports.push_back(checked_port(*value, "port"));
      }
      return ports;
    }
    const auto base = parse_u64_strict(*port_value);
    if (!base.has_value()) {
      throw std::runtime_error("bad port value in config: " + *port_value);
    }
    fallback_base_port = checked_port(*base, "port");
  }

  return {fallback_base_port};
}

std::vector<std::string> parse_line_hosts(const IniConfig& config, const NodeOptions& options) {
  if (auto hosts = config.get_any("common", {"ips", "hosts", "line_ips"})) {
    return split_list(*hosts);
  }
  if (auto hosts = config.get_any(options.mode, {"ip", "ips", "host", "hosts"})) {
    return split_list(*hosts);
  }
  return {options.host};
}

std::vector<std::uint64_t> parse_bandwidth_limits(const IniConfig& config) {
  const auto text = config.get_any("common", {"bandwidth_limit", "bandwidth_limits", "line_limits"});
  if (!text.has_value()) {
    return {};
  }
  std::vector<std::uint64_t> limits;
  for (const auto& part : split_list(*text)) {
    const auto value = parse_size_bytes(part);
    if (!value.has_value() || *value == 0) {
      throw std::runtime_error("bad Bandwidth_Limit value in config: " + part);
    }
    limits.push_back(*value);
  }
  return limits;
}

bool apply_size_option(const IniConfig& config,
                       std::initializer_list<std::string_view> keys,
                       std::uint64_t& target) {
  const auto text = config.get_any("common", keys);
  if (!text.has_value() || trim(*text).empty()) {
    return false;
  }
  const auto value = parse_size_bytes(*text);
  if (!value.has_value() || *value == 0) {
    throw std::runtime_error("bad size value in config: " + *text);
  }
  target = *value;
  return true;
}

bool apply_u64_option(const IniConfig& config,
                      std::initializer_list<std::string_view> keys,
                      std::uint64_t& target) {
  const auto text = config.get_any("common", keys);
  if (!text.has_value() || trim(*text).empty()) {
    return false;
  }
  const auto value = parse_u64_strict(*text);
  if (!value.has_value()) {
    throw std::runtime_error("bad integer value in config: " + *text);
  }
  target = *value;
  return true;
}

bool apply_ms_option(const IniConfig& config,
                     std::initializer_list<std::string_view> keys,
                     std::chrono::milliseconds& target) {
  std::uint64_t value = 0;
  if (!apply_u64_option(config, keys, value)) {
    return false;
  }
  if (value == 0 || value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
    throw std::runtime_error("millisecond config value must be positive and in range");
  }
  target = std::chrono::milliseconds(static_cast<std::int64_t>(value));
  return true;
}

void apply_runtime_config(NodeOptions& options, const IniConfig& config) {
  const bool recv_window_configured =
      apply_size_option(config, {"recv_window", "recv_window_bytes", "receive_window", "receive_window_bytes"},
                        options.recv_window_bytes);
  apply_size_option(config, {"chunk_size", "chunk_bytes"}, options.chunk_size);
  apply_u64_option(config, {"heartbeat_timeout_ticks"}, options.heartbeat_timeout_ticks);
  apply_u64_option(config, {"chunk_retransmit_ticks", "retransmit_ticks"}, options.chunk_retransmit_ticks);
  apply_u64_option(config,
                   {"max_retransmit_retries", "retransmit_retries"},
                   options.max_retransmit_retries);
  apply_u64_option(config,
                   {"max_manifest_recovery_attempts", "manifest_recovery_attempts"},
                   options.max_manifest_recovery_attempts);
  apply_u64_option(config,
                   {"max_missing_ranges_per_heartbeat", "max_missing_ranges"},
                   options.max_missing_ranges_per_heartbeat);
  apply_u64_option(config, {"heartbeat_ack_batch_size", "ack_batch_size"}, options.heartbeat_ack_batch_size);
  apply_u64_option(config, {"reconnect_base_delay_ms", "reconnect_base_ms"}, options.reconnect_base_delay_ms);
  apply_u64_option(config, {"reconnect_max_delay_ms", "reconnect_max_ms"}, options.reconnect_max_delay_ms);
  apply_ms_option(config,
                  {"heartbeat_interval_ms", "receiver_heartbeat_interval_ms"},
                  options.receiver_heartbeat_interval);
  apply_ms_option(config,
                  {"commit_poll_interval_ms", "receiver_commit_poll_interval_ms"},
                  options.receiver_commit_poll_interval);
  apply_ms_option(config,
                  {"watch_interval_ms", "watch_poll_interval_ms"},
                  options.watch_poll_interval);
  apply_ms_option(config,
                  {"watch_rescan_debounce_ms", "rescan_debounce_ms"},
                  options.watch_rescan_debounce);

  if (options.chunk_size == 0 || options.chunk_size + 1024 > kMaxFrameBytes) {
    throw std::runtime_error("common.chunk_size must be positive and leave room inside max frame");
  }
  if (!recv_window_configured) {
    options.recv_window_bytes = std::max<std::uint64_t>(options.recv_window_bytes, options.chunk_size * 2);
  }
  if (options.recv_window_bytes < options.chunk_size) {
    throw std::runtime_error("common.recv_window_bytes must be at least common.chunk_size");
  }
  if (options.reconnect_base_delay_ms == 0 || options.reconnect_max_delay_ms == 0) {
    throw std::runtime_error("reconnect delay config values must be positive");
  }
  if (options.reconnect_max_delay_ms < options.reconnect_base_delay_ms) {
    throw std::runtime_error("reconnect_max_delay_ms must be >= reconnect_base_delay_ms");
  }
  if (options.watch_poll_interval.count() <= 0 || options.watch_rescan_debounce.count() <= 0) {
    throw std::runtime_error("watch interval config values must be positive");
  }
}

std::uint32_t resolve_line_count(const NodeOptions& options,
                                 const IniConfig& config,
                                 std::size_t host_count,
                                 std::size_t port_count,
                                 std::size_t limit_count) {
  std::uint64_t lines = options.lines;
  if (auto value = config.get_any("common", {"link_num", "line_num", "lines"})) {
    const auto parsed = parse_u64_strict(*value);
    if (!parsed.has_value() || *parsed == 0) {
      throw std::runtime_error("common.link_num must be a positive integer");
    }
    lines = *parsed;
  }
  lines = std::max<std::uint64_t>(lines, host_count);
  lines = std::max<std::uint64_t>(lines, port_count);
  lines = std::max<std::uint64_t>(lines, limit_count);
  if (lines == 0 || lines > std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("line count is out of range");
  }
  return static_cast<std::uint32_t>(lines);
}

std::vector<StreamRootConfig> parse_stream_roots(std::string_view value,
                                                 std::uint64_t& next_stream_id) {
  std::vector<StreamRootConfig> roots;
  for (const auto& item : split_list(value)) {
    const auto regex_delim = item.find("::");
    const auto path_and_id = regex_delim == std::string::npos
                                 ? item
                                 : item.substr(0, regex_delim);
    const auto regex_text = regex_delim == std::string::npos
                                ? std::string{}
                                : item.substr(regex_delim + 2);
    const auto colon = path_and_id.find(':');
    if (colon == std::string::npos) {
      roots.push_back(StreamRootConfig{
          .stream_id = next_stream_id++,
          .root = path_and_id,
          .entry_name_regex = regex_text,
      });
      continue;
    }
    const auto id_text = trim(std::string_view(path_and_id).substr(0, colon));
    const auto path_text = trim(std::string_view(path_and_id).substr(colon + 1));
    const auto id = parse_u64_strict(id_text);
    if (!id.has_value() || *id == 0 || path_text.empty()) {
      throw std::runtime_error("bad stream root config: " + item);
    }
    roots.push_back(StreamRootConfig{
        .stream_id = *id,
        .root = path_text,
        .entry_name_regex = regex_text,
    });
  }
  return roots;
}

void append_stream_roots(std::vector<StreamRootConfig>& out,
                         std::string_view value,
                         std::uint64_t& next_stream_id) {
  auto roots = parse_stream_roots(value, next_stream_id);
  out.insert(out.end(),
             std::make_move_iterator(roots.begin()),
             std::make_move_iterator(roots.end()));
}

void apply_config_file(NodeOptions& options, const std::filesystem::path& path) {
  const auto config = load_ini_config(path);

  if (auto value = config.get_any("common", {"compress", "compression"})) {
    options.compression = parse_compression(*value);
  }
  if (auto value = config.get_any("common", {"checksum"})) {
    options.checksum_algo = parse_checksum(*value);
  }
  if (auto value = config.get_any("common", {"watch", "watch_enabled"})) {
    options.watch = parse_bool(*value);
  }
  if (auto value = config.get_any("common", {"watch_backend"})) {
    options.watch_backend = parse_watch_backend(*value);
  }
  apply_runtime_config(options, config);

  if (auto value = config.get_any("receiver", {"mount_dir", "root"})) {
    if (!trim(*value).empty()) {
      options.root = trim(*value);
    }
  }
  if (auto value = config.get_any("sender", {"source_root", "root"})) {
    if (!trim(*value).empty()) {
      options.source_root = trim(*value);
    }
  }
  std::uint64_t next_stream_id = kStreamId;
  options.source_streams.clear();
  if (auto value = config.get_any("sender", {"high_prioity_dirs", "high_priority_dirs"})) {
    append_stream_roots(options.source_streams, *value, next_stream_id);
  }
  if (auto value = config.get_any("sender", {"low_prioirt_dies", "low_priority_dirs"})) {
    append_stream_roots(options.source_streams, *value, next_stream_id);
  }
  if (!options.source_streams.empty()) {
    options.source_root = options.source_streams.front().root;
  }

  auto hosts = parse_line_hosts(config, options);
  if (hosts.empty()) {
    hosts.push_back(options.host);
  }
  const auto ports = parse_ports(config, options.mode, options.base_port);
  const auto limits = parse_bandwidth_limits(config);
  options.lines = resolve_line_count(options, config, hosts.size(), ports.size(), limits.size());

  if (hosts.size() == 1) {
    hosts.resize(options.lines, hosts.front());
  }
  if (hosts.size() != options.lines) {
    throw std::runtime_error("ip list size must be 1 or match link_num");
  }

  std::vector<std::uint16_t> expanded_ports;
  if (ports.size() == 1) {
    const auto base = ports.front();
    expanded_ports.reserve(options.lines);
    for (std::uint32_t i = 0; i < options.lines; ++i) {
      const auto port = static_cast<std::uint64_t>(base) + i;
      expanded_ports.push_back(checked_port(port, "port"));
    }
    options.base_port = base;
  } else if (ports.size() == options.lines) {
    expanded_ports = ports;
    options.base_port = ports.front();
  } else {
    throw std::runtime_error("port list size must be 1 or match link_num");
  }

  options.host = hosts.front();
  options.line_endpoints.clear();
  options.line_configs.clear();
  for (std::uint32_t i = 1; i <= options.lines; ++i) {
    const auto index = static_cast<std::size_t>(i - 1);
    const auto budget = limits.empty()
                            ? kLineBudgetBytes
                            : limits[std::min<std::size_t>(index, limits.size() - 1)];
    options.line_endpoints.push_back(network::LineEndpoint{
        .id = i,
        .protocol = network::Protocol::Tcp,
        .endpoint = Endpoint{hosts[index], expanded_ports[index]},
        .name = "tcp-line-" + std::to_string(i),
    });
    options.line_configs.push_back(LineConfig{
        .id = i,
        .name = "tcp-line-" + std::to_string(i),
        .limiter = TokenBucketConfig{
            .tokens_per_tick = budget,
            .capacity = budget,
            .tick = std::chrono::milliseconds(10),
        },
        .initial_recv_window_bytes = options.recv_window_bytes,
        .heartbeat_timeout_ticks = options.heartbeat_timeout_ticks,
        .initially_connected = false,
    });
  }
}

void print_usage() {
  std::cerr
      << "usage:\n"
      << "  yisync_node receiver --config config.txt\n"
      << "  yisync_node sender --config config.txt\n"
      << "  yisync_node receiver --host 127.0.0.1 --base-port 19000 --lines 2 --root /tmp/yisync_receiver\n"
      << "  yisync_node sender --host 127.0.0.1 --base-port 19000 --lines 2 --size 153600\n"
      << "  yisync_node sender --host 127.0.0.1 --base-port 19000 --lines 2 --source-root /tmp/yisync_source\n"
      << "  add --watch to keep sender/receiver running and rescan source dirs on file events\n";
}

}  // namespace

std::vector<LineConfig> make_line_configs(const NodeOptions& options) {
  if (!options.line_configs.empty()) {
    return options.line_configs;
  }

  std::vector<LineConfig> configs;
  configs.reserve(options.lines);
  for (std::uint32_t i = 1; i <= options.lines; ++i) {
    configs.push_back(LineConfig{
        .id = i,
        .name = "tcp-line-" + std::to_string(i),
        .limiter = TokenBucketConfig{
            .tokens_per_tick = kLineBudgetBytes,
            .capacity = kLineBudgetBytes,
            .tick = std::chrono::milliseconds(10),
        },
        .initial_recv_window_bytes = options.recv_window_bytes,
        .heartbeat_timeout_ticks = options.heartbeat_timeout_ticks,
        .initially_connected = false,
    });
  }
  return configs;
}

std::vector<network::LineEndpoint> make_line_endpoints(const NodeOptions& options) {
  if (!options.line_endpoints.empty()) {
    return options.line_endpoints;
  }

  std::vector<network::LineEndpoint> endpoints;
  endpoints.reserve(options.lines);
  for (std::uint32_t i = 1; i <= options.lines; ++i) {
    endpoints.push_back(network::LineEndpoint{
        .id = i,
        .protocol = network::Protocol::Tcp,
        .endpoint = Endpoint{options.host, line_port(options, i)},
        .name = "tcp-line-" + std::to_string(i),
    });
  }
  return endpoints;
}

NodeOptions parse_options(int argc, char** argv) {
  if (argc < 2) {
    print_usage();
    throw std::runtime_error("missing mode");
  }

  NodeOptions options;
  int first_option = 2;
  options.mode = argv[1];
  if (options.mode == "--config") {
    if (argc < 4) {
      print_usage();
      throw std::runtime_error("--config requires a path and mode");
    }
    options.config_path = argv[2];
    options.mode = argv[3];
    first_option = 4;
  }
  if (options.mode == "source") {
    options.mode = "sender";
  } else if (options.mode == "sink") {
    options.mode = "receiver";
  }

  for (int i = first_option; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (arg != "--config") {
      continue;
    }
    if (i + 1 >= argc) {
      throw std::runtime_error("missing value for --config");
    }
    options.config_path = argv[++i];
  }
  if (!options.config_path.empty()) {
    apply_config_file(options, options.config_path);
  }

  for (int i = first_option; i < argc; ++i) {
    const std::string_view arg = argv[i];
    const auto need_value = [&](std::string_view name) -> std::string {
      if (i + 1 >= argc) {
        throw std::runtime_error("missing value for " + std::string(name));
      }
      return argv[++i];
    };

    if (arg == "--config") {
      (void)need_value(arg);
    } else if (arg == "--host") {
      options.host = need_value(arg);
      options.line_endpoints.clear();
    } else if (arg == "--base-port") {
      options.base_port = static_cast<std::uint16_t>(std::stoul(need_value(arg)));
      options.line_endpoints.clear();
    } else if (arg == "--lines") {
      options.lines = static_cast<std::uint32_t>(std::stoul(need_value(arg)));
      options.line_endpoints.clear();
      options.line_configs.clear();
    } else if (arg == "--root") {
      options.root = need_value(arg);
    } else if (arg == "--source-root") {
      options.source_root = need_value(arg);
      options.source_streams.clear();
    } else if (arg == "--size") {
      options.size = static_cast<std::uint64_t>(std::stoull(need_value(arg)));
    } else if (arg == "--drop-line-once") {
      options.drop_line_once = static_cast<LineId>(std::stoul(need_value(arg)));
    } else if (arg == "--watch") {
      options.watch = true;
    } else if (arg == "--watch-backend") {
      options.watch_backend = parse_watch_backend(need_value(arg));
    } else {
      throw std::runtime_error("unknown option: " + std::string(arg));
    }
  }

  if (options.mode != "receiver" && options.mode != "sender") {
    throw std::runtime_error("mode must be receiver or sender");
  }
  if (options.lines == 0) {
    throw std::runtime_error("at least one line is required");
  }
  return options;
}

}  // namespace yisync::node
