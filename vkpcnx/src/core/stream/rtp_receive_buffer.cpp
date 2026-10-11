#include "core/stream/rtp_receive_buffer.hpp"

#include <algorithm>
#include <cstddef>

namespace vkpcnx::stream {

namespace {

constexpr size_t MAX_NACK_PARTS = 64;  // FCI entries per RTCP packet, keeps it well under the MTU
constexpr int OLD_STREAK_RESTART = 64; // that many media packets behind in a row = sender restarted

const rtc::RtpHeader *rtpHeader(const rtc::Message &m) {
  return reinterpret_cast<const rtc::RtpHeader *>(m.data());
}

// Offset of the payload, or 0 if the header is truncated
size_t payloadOffset(const rtc::Message &m) {
  auto *h = rtpHeader(m);
  if (m.size() < h->getSize() || (h->extension() && m.size() < h->getSize() + 4))
    return 0;
  size_t offset = h->getSize() + h->getExtensionHeaderSize();
  return offset <= m.size() ? offset : 0;
}

// Payload bytes without the padding, or -1 if malformed
long payloadSize(const rtc::Message &m) {
  size_t offset = payloadOffset(m);
  if (!offset)
    return -1;
  size_t padding = rtpHeader(m)->padding() ? std::to_integer<size_t>(m.back()) : 0;
  if (offset + padding > m.size())
    return -1;
  return static_cast<long>(m.size() - offset - padding);
}

} // namespace

RtpReceiveBuffer::RtpReceiveBuffer(std::vector<int> mediaPts, std::map<int, int> rtxPts)
    : mediaPts_(std::move(mediaPts)), rtxPts_(std::move(rtxPts)) {}

void RtpReceiveBuffer::incoming(rtc::message_vector &messages, const rtc::message_callback &send) {
  incomingAt(messages, send, Clock::now());
}

void RtpReceiveBuffer::incomingAt(
  rtc::message_vector &messages, const rtc::message_callback &send, Clock::time_point now
) {
  rtc::message_vector result;
  for (auto &message : messages) {
    // RTCP, and anything too short to be RTP, goes on unchanged (downstream drops the latter)
    if (message->type != rtc::Message::Binary || message->size() < sizeof(rtc::RtpHeader)) {
      result.push_back(std::move(message));
      continue;
    }
    int pt = rtpHeader(*message)->payloadType();
    if (auto rtx = rtxPts_.find(pt); rtx != rtxPts_.end()) {
      if (auto original = unwrapRtx(message, rtx->second)) {
        rtxSeen_ = true;
        insert(std::move(original), true, now);
      }
      continue;
    }
    if (!isMedia(pt)) {
      result.push_back(std::move(message));
      continue;
    }
    uint32_t ssrc = rtpHeader(*message)->ssrc();
    if (started_ && ssrc != mediaSsrc_)
      restart();
    mediaSsrc_ = ssrc;
    insert(std::move(message), false, now);
  }
  release(now);
  sendNacks(send, now);

  for (auto &m : ready_)
    result.push_back(std::move(m));
  ready_.clear();
  messages.swap(result);

  if (!delta_.empty() && onCounters)
    onCounters(delta_);
  delta_ = {};
  if (lostInCall_ && onLoss)
    onLoss();
  lostInCall_ = false;
}

bool RtpReceiveBuffer::isMedia(int pt) const {
  return std::find(mediaPts_.begin(), mediaPts_.end(), pt) != mediaPts_.end();
}

int64_t RtpReceiveBuffer::unwrap(uint16_t seq) const {
  if (!started_)
    return seq;
  return highest_ +
         static_cast<int16_t>(static_cast<uint16_t>(seq - static_cast<uint16_t>(highest_)));
}

rtc::message_ptr RtpReceiveBuffer::unwrapRtx(const rtc::message_ptr &message, int mediaPt) const {
  if (!started_)
    return nullptr; // the media SSRC isn't known yet
  // RFC 4588: payload = original sequence number (2 bytes) + original payload.
  // Padding-only packets are bandwidth probes.
  long size = payloadSize(*message);
  if (size <= 2)
    return nullptr;
  size_t offset = payloadOffset(*message);
  auto osnHigh = std::to_integer<uint16_t>((*message)[offset]);
  auto osnLow = std::to_integer<uint16_t>((*message)[offset + 1]);

  rtc::binary data;
  data.reserve(offset + static_cast<size_t>(size) - 2);
  data.insert(data.end(), message->begin(), message->begin() + static_cast<long>(offset));
  data.insert(
    data.end(),
    message->begin() + static_cast<long>(offset) + 2,
    message->begin() + static_cast<long>(offset) + size
  );
  auto original = rtc::make_message(std::move(data), rtc::Message::Binary, message->stream);
  auto *h = reinterpret_cast<rtc::RtpHeader *>(original->data());
  h->_first &= static_cast<uint8_t>(~0x20); // padding was dropped above
  h->setSeqNumber(static_cast<uint16_t>(osnHigh << 8 | osnLow));
  h->setPayloadType(static_cast<uint8_t>(mediaPt));
  h->setSsrc(mediaSsrc_);
  return original;
}

void RtpReceiveBuffer::insert(
  rtc::message_ptr message, bool retransmission, Clock::time_point now
) {
  int64_t seq = unwrap(rtpHeader(*message)->seqNumber());
  if (!started_) {
    started_ = true;
    next_ = highest_ = seq;
  }

  if (seq < next_ || held_.count(seq)) {
    // Duplicate, or arrived after it was given up. A long run of these from
    // the media stream itself means the sender restarted its sequence lower.
    if (retransmission)
      delta_.duplicates++;
    else if (++oldStreak_ >= OLD_STREAK_RESTART) {
      restart();
      insert(std::move(message), retransmission, now);
    }
    return;
  }
  if (!retransmission)
    oldStreak_ = 0;

  if (seq > highest_ + static_cast<int64_t>(MAX_HELD)) {
    // A jump far ahead (sender restart): nothing before it is worth waiting for
    restart();
    insert(std::move(message), retransmission, now);
    return;
  }

  if (seq > highest_) {
    for (int64_t s = highest_ + 1; s < seq; s++)
      missing_[s] = Missing{now, now, 0};
    highest_ = seq;
  } else if (auto it = missing_.find(seq); it != missing_.end()) {
    auto lateness = now - it->second.since;
    missing_.erase(it);
    if (retransmission) {
      delta_.recovered++;
    } else {
      delta_.late++;
      delta_.lateMax = std::max(delta_.lateMax, lateness);
      addLateness(lateness);
    }
  }
  held_[seq] = payloadSize(*message) > 0 ? std::move(message) : nullptr;
}

void RtpReceiveBuffer::release(Clock::time_point now) {
  auto wait = nackIgnored_ ? reorderWait_ : Clock::duration(MAX_WAIT);
  while (!held_.empty()) {
    auto first = held_.begin();
    if (first->first == next_) {
      if (first->second) {
        ready_.push_back(std::move(first->second));
        delta_.received++;
      }
      held_.erase(first);
      next_++;
      continue;
    }
    // next_ is missing; the gap before the first held packet was found all at once
    auto gap = missing_.find(next_);
    bool expired = gap == missing_.end() || now - gap->second.since >= wait;
    if (!expired && held_.size() <= MAX_HELD)
      break;
    giveUpUntil(first->first);
  }
}

void RtpReceiveBuffer::giveUpUntil(int64_t seq) {
  auto end = missing_.lower_bound(seq);
  for (auto it = missing_.begin(); it != end; ++it) {
    delta_.lost++;
    if (it->second.nacks > 0)
      nackedGivenUp_++;
  }
  missing_.erase(missing_.begin(), end);
  if (seq > next_)
    lostInCall_ = true;
  next_ = std::max(next_, seq);

  if (!nackIgnored_ && !rtxSeen_ && nackedGivenUp_ >= NACK_PROBE) {
    nackIgnored_ = true;
    if (onNackIgnored)
      onNackIgnored();
  }
}

void RtpReceiveBuffer::addLateness(Clock::duration lateness) {
  if (lateness_.size() < LATENESS_WINDOW)
    lateness_.push_back(lateness);
  else
    lateness_[latenessNext_] = lateness;
  latenessNext_ = (latenessNext_ + 1) % LATENESS_WINDOW;
  if (++latenessFresh_ < LATENESS_REFRESH)
    return;
  latenessFresh_ = 0;

  auto sorted = lateness_;
  std::sort(sorted.begin(), sorted.end());
  auto percentile = [&](size_t p) { return sorted[(sorted.size() - 1) * p / 100]; };
  auto nackDelay = std::min(percentile(95), Clock::duration(MAX_NACK_DELAY));
  auto reorderWait = std::clamp(
    percentile(99) + std::chrono::milliseconds(10),
    Clock::duration(REORDER_WAIT),
    Clock::duration(MAX_WAIT)
  );
  using std::chrono::milliseconds;
  bool changed = std::chrono::abs(nackDelay - nackDelay_) >= milliseconds(2) ||
                 std::chrono::abs(reorderWait - reorderWait_) >= milliseconds(5);
  nackDelay_ = nackDelay;
  reorderWait_ = reorderWait;
  if (changed && onDelayChange)
    onDelayChange(nackDelay_, reorderWait_);
}

void RtpReceiveBuffer::restart() {
  for (auto &[seq, message] : held_)
    if (message) {
      ready_.push_back(std::move(message));
      delta_.received++;
    }
  if (!missing_.empty()) {
    delta_.lost += missing_.size();
    lostInCall_ = true;
  }
  held_.clear();
  missing_.clear();
  started_ = false;
  oldStreak_ = 0;
}

void RtpReceiveBuffer::sendNacks(const rtc::message_callback &send, Clock::time_point now) {
  if (nackIgnored_ || missing_.empty() || !send)
    return;

  std::vector<int64_t> seqs;
  for (auto &[seq, m] : missing_) {
    if (m.nacks >= MAX_NACKS)
      continue;
    if (m.nacks == 0 ? now - m.since < nackDelay_ : now - m.lastNack < NACK_INTERVAL)
      continue;
    m.nacks++;
    m.lastNack = now;
    seqs.push_back(seq);
  }
  if (seqs.empty())
    return;

  // Generic NACK FCI: a packet id plus a bitmask of the 16 that follow it
  std::vector<std::pair<uint16_t, uint16_t>> parts;
  for (size_t i = 0; i < seqs.size();) {
    int64_t pid = seqs[i++];
    uint16_t blp = 0;
    while (i < seqs.size() && seqs[i] - pid <= 16) {
      blp |= static_cast<uint16_t>(1u << (seqs[i] - pid - 1));
      i++;
    }
    parts.emplace_back(static_cast<uint16_t>(pid), blp);
  }
  for (size_t begin = 0; begin < parts.size(); begin += MAX_NACK_PARTS) {
    size_t count = std::min(MAX_NACK_PARTS, parts.size() - begin);
    auto message = rtc::make_message(rtc::RtcpNack::Size(count), rtc::Message::Control);
    auto *nack = reinterpret_cast<rtc::RtcpNack *>(message->data());
    nack->preparePacket(mediaSsrc_, static_cast<unsigned>(count));
    auto *fci = reinterpret_cast<rtc::RtcpNackPart *>(message->data() + rtc::RtcpNack::Size(0));
    for (size_t i = 0; i < count; i++) {
      fci[i].setPid(parts[begin + i].first);
      fci[i].setBlp(parts[begin + i].second);
    }
    send(message);
  }

  delta_.nacked += seqs.size();
}

} // namespace vkpcnx::stream
