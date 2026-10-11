#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <vector>

#include <rtc/rtc.hpp>

namespace vkpcnx::stream {

// Receive side of video loss recovery, chained so it sees the raw RTP packets
// first (before RtcpReceivingSession and the H.264 depacketizer):
//   - packets that arrive after a sequence gap are held and released in
//     order, so a late (reordered) packet no longer breaks two frames;
//   - missing packets are asked for with RTCP generic NACKs (RFC 4585), and
//     the RTX retransmissions (RFC 4588) are put back into the stream as the
//     original packets; RTX padding (bandwidth probes) is dropped. The first
//     NACK waits until 95% of reordered packets would have arrived (Switch
//     Wi-Fi delivers ~30% of them late), so late ones aren't sent twice;
//   - a packet still missing after MAX_WAIT is given up: only then is it
//     lost, and the decoder has to wait for a keyframe.
// Before this every gap made the decoder skip everything until the next
// keyframe (rate-limited to one PLI a second), so with a few percent loss
// fast scenes, which take many packets per frame, played at ~1 fps.
//
// Not thread-safe: libdatachannel calls incoming() from one thread per track.
class RtpReceiveBuffer final : public rtc::MediaHandler {
public:
  using Clock = std::chrono::steady_clock;

  static constexpr std::chrono::milliseconds NACK_INTERVAL{40};
  static constexpr int MAX_NACKS = 3;
  // Upper bound of the adaptive delay before the first NACK
  static constexpr std::chrono::milliseconds MAX_NACK_DELAY{50};
  // How long a gap holds back the packets after it (the offer's rtx-time is 125 ms)
  static constexpr std::chrono::milliseconds MAX_WAIT{150};
  // Once NACK_PROBE NACKed packets were given up without any RTX ever arriving,
  // stop asking and only wait out reordering (at least REORDER_WAIT)
  static constexpr std::chrono::milliseconds REORDER_WAIT{30};
  static constexpr uint64_t NACK_PROBE = 50;
  // Lateness of reordered packets: window kept, and how often its percentiles are refreshed
  static constexpr size_t LATENESS_WINDOW = 256;
  static constexpr size_t LATENESS_REFRESH = 64;
  static constexpr size_t MAX_HELD = 2048; // packets; also the largest gap that is waited for

  struct Counters {
    uint64_t received = 0;  // media packets released, retransmissions included
    uint64_t lost = 0;      // given up
    uint64_t recovered = 0; // filled by an RTX retransmission
    uint64_t late = 0;      // filled by the original packet arriving out of order
    uint64_t nacked = 0;    // sequence numbers asked for (repeats included)
    uint64_t duplicates = 0; // RTX copies of packets that had arrived already
    Clock::duration lateMax{}; // the latest of the `late` packets, after its gap was seen

    bool empty() const {
      return !received && !lost && !recovered && !late && !nacked && !duplicates;
    }
  };

  // mediaPts: the H.264 payload types; rtxPts: RTX payload type → media payload type
  RtpReceiveBuffer(std::vector<int> mediaPts, std::map<int, int> rtxPts);

  // What changed during one incoming() call; only called when something did
  std::function<void(const Counters &delta)> onCounters;
  // Packets were given up (after onCounters)
  std::function<void()> onLoss;
  // The server never answered the NACKs; recovery is off from now on
  std::function<void()> onNackIgnored;
  // The delay before a first NACK (or, after onNackIgnored, the reorder wait) changed
  std::function<void(Clock::duration nackDelay, Clock::duration reorderWait)> onDelayChange;

  void incoming(rtc::message_vector &messages, const rtc::message_callback &send) override;
  // incoming() with an explicit clock, for tests
  void incomingAt(
    rtc::message_vector &messages, const rtc::message_callback &send, Clock::time_point now
  );

private:
  struct Missing {
    Clock::time_point since;
    Clock::time_point lastNack;
    int nacks = 0;
  };

  bool isMedia(int pt) const;
  // RTX → original packet; null for padding or before the media SSRC is known
  rtc::message_ptr unwrapRtx(const rtc::message_ptr &message, int mediaPt) const;
  void insert(rtc::message_ptr message, bool retransmission, Clock::time_point now);
  void release(Clock::time_point now);
  void sendNacks(const rtc::message_callback &send, Clock::time_point now);
  void giveUpUntil(int64_t seq);
  void addLateness(Clock::duration lateness);
  // Sender restarted (new SSRC, or a sequence jump): flush and start over
  void restart();
  int64_t unwrap(uint16_t seq) const;

  std::vector<int> mediaPts_;
  std::map<int, int> rtxPts_;

  bool started_ = false;
  uint32_t mediaSsrc_ = 0;
  int64_t next_ = 0;    // next extended sequence number to release
  int64_t highest_ = 0; // highest extended sequence number seen
  // Received but not released yet; null = padding (takes a sequence number, carries nothing)
  std::map<int64_t, rtc::message_ptr> held_;
  std::map<int64_t, Missing> missing_;
  rtc::message_vector ready_; // released during this incoming() call
  int oldStreak_ = 0;

  bool nackIgnored_ = false;
  bool rtxSeen_ = false;          // any RTX payload, duplicates included
  uint64_t nackedGivenUp_ = 0;   // NACKed, then given up
  Clock::duration nackDelay_{};  // 95th percentile of the lateness, capped
  Clock::duration reorderWait_ = REORDER_WAIT; // 99th percentile + margin, once NACKs are off
  std::vector<Clock::duration> lateness_; // ring of the last LATENESS_WINDOW
  size_t latenessNext_ = 0, latenessFresh_ = 0;
  Counters delta_;
  bool lostInCall_ = false;
};

} // namespace vkpcnx::stream
