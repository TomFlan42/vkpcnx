// Unit tests for RtpReceiveBuffer (reordering, NACK/RTX recovery, loss).
// Build: see VKPCNX_BUILD_TESTS in CMakeLists.txt

#include <algorithm>
#include <cstdio>
#include <vector>

#include "core/stream/rtp_receive_buffer.hpp"

using namespace vkpcnx::stream;
using Clock = RtpReceiveBuffer::Clock;
using namespace std::chrono_literals;

static int failures = 0;
#define CHECK(cond)                                                                                \
  do {                                                                                             \
    if (!(cond)) {                                                                                 \
      std::printf("FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                                  \
      failures++;                                                                                  \
    }                                                                                              \
  } while (0)

static constexpr int PT = 102, RTX_PT = 103;
static constexpr uint32_t SSRC = 0x11111111, RTX_SSRC = 0x22222222;

static rtc::message_ptr rtp(int pt, uint32_t ssrc, uint16_t seq, std::vector<uint8_t> payload) {
  rtc::binary data(12);
  data[0] = std::byte{0x80};
  data[1] = std::byte(pt);
  data[2] = std::byte(seq >> 8);
  data[3] = std::byte(seq & 0xff);
  for (int i = 0; i < 4; i++)
    data[8 + i] = std::byte((ssrc >> (24 - 8 * i)) & 0xff);
  for (uint8_t b : payload)
    data.push_back(std::byte{b});
  return rtc::make_message(std::move(data));
}

static rtc::message_ptr media(uint16_t seq) { return rtp(PT, SSRC, seq, {uint8_t(seq), 0xaa}); }

// RFC 4588 retransmission of `seq`, with its own sequence number `rtxSeq`
static rtc::message_ptr rtx(uint16_t rtxSeq, uint16_t seq) {
  return rtp(
    RTX_PT, RTX_SSRC, rtxSeq, {uint8_t(seq >> 8), uint8_t(seq & 0xff), uint8_t(seq), 0xaa}
  );
}

static uint16_t seqOf(const rtc::message_ptr &m) {
  return reinterpret_cast<const rtc::RtpHeader *>(m->data())->seqNumber();
}

struct Harness {
  RtpReceiveBuffer buffer{{PT}, {{RTX_PT, PT}}};
  Clock::time_point now = Clock::time_point{} + 1h;
  std::vector<std::vector<uint16_t>> nacks; // sequence numbers of each NACK sent
  RtpReceiveBuffer::Counters total;
  int lossCalls = 0;
  bool nackIgnored = false;

  Harness() {
    buffer.onCounters = [this](const RtpReceiveBuffer::Counters &d) {
      total.received += d.received;
      total.lost += d.lost;
      total.recovered += d.recovered;
      total.late += d.late;
      total.nacked += d.nacked;
      total.duplicates += d.duplicates;
      total.lateMax = std::max(total.lateMax, d.lateMax);
    };
    buffer.onLoss = [this] { lossCalls++; };
    buffer.onNackIgnored = [this] { nackIgnored = true; };
  }

  // Feeds the packets in one batch, returns the sequence numbers released
  std::vector<uint16_t> feed(rtc::message_vector in) {
    buffer.incomingAt(in, [this](rtc::message_ptr m) { onSend(m); }, now);
    std::vector<uint16_t> seqs;
    for (auto &m : in) {
      CHECK(m->type == rtc::Message::Binary);
      seqs.push_back(seqOf(m));
    }
    released.insert(released.end(), in.begin(), in.end());
    return seqs;
  }

  void onSend(const rtc::message_ptr &m) {
    CHECK(m->type == rtc::Message::Control);
    auto *nack = reinterpret_cast<rtc::RtcpNack *>(m->data());
    CHECK(nack->header.header.payloadType() == 205);
    CHECK(nack->header.mediaSourceSSRC() == SSRC);
    CHECK(m->size() == rtc::RtcpNack::Size(nack->getSeqNoCount()));
    std::vector<uint16_t> seqs;
    auto *parts = reinterpret_cast<rtc::RtcpNackPart *>(m->data() + rtc::RtcpNack::Size(0));
    for (unsigned i = 0; i < nack->getSeqNoCount(); i++)
      for (uint16_t s : parts[i].getSequenceNumbers())
        seqs.push_back(s);
    nacks.push_back(seqs);
  }

  std::vector<rtc::message_ptr> released;
};

using Seqs = std::vector<uint16_t>;

static void testInOrder() {
  Harness h;
  CHECK((h.feed({media(1), media(2)}) == Seqs{1, 2}));
  CHECK((h.feed({media(3)}) == Seqs{3}));
  CHECK(h.nacks.empty());
  CHECK(h.total.received == 3 && h.total.lost == 0);
}

static void testReorder() {
  Harness h;
  CHECK((h.feed({media(1), media(2), media(4)}) == Seqs{1, 2}));
  CHECK((h.nacks == std::vector<Seqs>{{3}})); // asked right away
  h.now += 5ms;
  CHECK((h.feed({media(3)}) == Seqs{3, 4}));
  CHECK((h.feed({media(5)}) == Seqs{5}));
  CHECK(h.total.late == 1 && h.total.lost == 0 && h.lossCalls == 0);
  // Ordering inside one batch
  CHECK((h.feed({media(7), media(6)}) == Seqs{6, 7}));
}

static void testRtxRecovery() {
  Harness h;
  h.feed({media(10), media(11), media(13), media(14)});
  CHECK((h.nacks == std::vector<Seqs>{{12}}));
  h.now += 60ms;
  CHECK((h.feed({rtx(500, 12)}) == Seqs{12, 13, 14}));
  auto &m = h.released[2];
  auto *hdr = reinterpret_cast<const rtc::RtpHeader *>(m->data());
  CHECK(hdr->payloadType() == PT && hdr->ssrc() == SSRC && hdr->seqNumber() == 12);
  CHECK(m->size() == 14 && std::to_integer<int>((*m)[12]) == 12); // OSN stripped
  CHECK(h.total.recovered == 1 && h.total.lost == 0 && h.lossCalls == 0);
  // A second copy of it is a duplicate
  CHECK(h.feed({rtx(501, 12)}).empty());
}

static void testGiveUp() {
  Harness h;
  h.feed({media(1), media(3)});
  h.now += 40ms;
  CHECK(h.feed({media(4)}).empty());
  CHECK(h.nacks.size() == 2); // repeated after NACK_INTERVAL
  h.now += 40ms;
  h.feed({media(5)});
  h.now += 40ms;
  h.feed({media(6)});
  CHECK(h.nacks.size() == 3); // MAX_NACKS
  h.now += 40ms;              // 160 ms > MAX_WAIT
  CHECK((h.feed({media(7)}) == Seqs{3, 4, 5, 6, 7}));
  CHECK(h.total.lost == 1 && h.lossCalls == 1);
  // The original showing up afterwards is dropped
  CHECK(h.feed({media(2)}).empty());
}

static void testNackBitmask() {
  Harness h;
  h.feed({media(1), media(2), media(5), media(7), media(30)});
  CHECK(h.nacks.size() == 1);
  Seqs expected{3, 4, 6};
  for (uint16_t s = 8; s < 30; s++)
    expected.push_back(s);
  CHECK(h.nacks[0] == expected);
}

static void testWraparound() {
  Harness h;
  CHECK((h.feed({media(65534), media(65535), media(1)}) == Seqs{65534, 65535}));
  CHECK((h.nacks == std::vector<Seqs>{{0}}));
  CHECK((h.feed({rtx(9, 0)}) == Seqs{0, 1}));
  CHECK(h.total.lost == 0);
}

static void testRtxPaddingAndUnknown() {
  Harness h;
  CHECK(h.feed({rtx(1, 5)}).empty()); // before the media SSRC is known
  h.feed({media(1)});
  auto padding = rtp(RTX_PT, RTX_SSRC, 2, {0, 0, 0, 4});
  padding->at(0) |= std::byte{0x20}; // padding bit, 4 bytes of padding
  CHECK(h.feed({padding}).empty());
  // Audio and RTCP go through untouched
  auto other = rtp(111, 0x333, 77, {1});
  CHECK((h.feed({other}) == Seqs{77}));
  CHECK(h.total.received == 1 && h.total.lost == 0);
}

static void testNackIgnored() {
  Harness h;
  uint16_t seq = 1;
  h.feed({media(seq++)});
  // Every other packet lost, never retransmitted
  while (!h.nackIgnored && seq < 2000) {
    seq++;
    h.feed({media(seq++)});
    h.now += 200ms;
  }
  CHECK(h.nackIgnored);
  size_t sent = h.nacks.size();
  // From now on only reordering is waited for, and nothing is asked
  h.feed({media(seq + 1)});
  CHECK(h.nacks.size() == sent);
  h.now += RtpReceiveBuffer::REORDER_WAIT;
  CHECK((h.feed({media(seq + 2)}) == Seqs{uint16_t(seq + 1), uint16_t(seq + 2)}));
}

static void testNackAnswered() {
  Harness h;
  h.feed({media(1), media(3)});
  h.feed({rtx(1, 2)}); // the server retransmits
  uint16_t seq = 4;
  for (int i = 0; i < 1000; i++) {
    seq++;
    h.feed({media(seq++)});
    h.now += 200ms;
  }
  CHECK(!h.nackIgnored);
}

// Switch Wi-Fi: ~30% of packets late, none lost. The NACKs are answered by
// the late originals, which must not count as the server ignoring them.
static void testHeavyReorderKeepsNack() {
  Harness h;
  uint16_t b = 1;
  h.feed({media(b++)});
  // 60 ms late: past MAX_NACK_DELAY, so every one is NACKed before it shows up
  for (int i = 0; i < 1000; i++, b += 2) {
    h.feed({media(uint16_t(b + 1))});
    h.now += 55ms;
    h.feed({});
    h.now += 5ms;
    h.feed({media(b)});
  }
  CHECK(h.total.nacked >= 1000);
  CHECK(!h.nackIgnored);
  CHECK(h.total.lost == 0 && h.total.late == 1000);
}

static void testAdaptiveNackDelay() {
  Harness h;
  RtpReceiveBuffer::Clock::duration delay{};
  h.buffer.onDelayChange = [&](auto nackDelay, auto) { delay = nackDelay; };
  uint16_t b = 1;
  h.feed({media(b++)});
  // Every other packet 10 ms late
  for (size_t i = 0; i < RtpReceiveBuffer::LATENESS_REFRESH; i++, b += 2) {
    h.feed({media(uint16_t(b + 1))});
    h.now += 10ms;
    h.feed({media(b)});
    h.now += 1ms;
  }
  CHECK(delay == 10ms);
  CHECK(h.total.lateMax == 10ms);
  size_t sent = h.nacks.size();
  // A new gap is NACKed only once a late packet would have shown up
  h.feed({media(uint16_t(b + 1))});
  h.now += 9ms;
  h.feed({});
  CHECK(h.nacks.size() == sent);
  h.now += 1ms;
  h.feed({});
  CHECK((h.nacks.size() == sent + 1 && h.nacks.back() == Seqs{b}));
  // The original wins the race; the retransmission that follows is a duplicate
  h.feed({media(b)});
  h.feed({rtx(900, b)});
  CHECK(h.total.duplicates == 1 && h.total.lost == 0);
}

static void testRestart() {
  Harness h;
  h.feed({media(100), media(102)});
  // New SSRC: the held packet comes out, the new stream starts clean
  auto fresh = rtp(PT, 0x999, 5, {1});
  CHECK((h.feed({fresh}) == Seqs{102, 5}));
  CHECK(h.total.lost == 1);
  // Sequence jump on the same SSRC
  CHECK((h.feed({rtp(PT, 0x999, 20000, {1})}) == Seqs{20000}));
  // Restart further back than a wrap can explain: dropped until it's clearly a new run
  uint16_t seq = 55536; // 30000 behind 20000
  for (int i = 0; i < 63; i++)
    CHECK(h.feed({rtp(PT, 0x999, seq++, {1})}).empty());
  CHECK((h.feed({rtp(PT, 0x999, seq, {1})}) == Seqs{seq}));
}

int main() {
  testInOrder();
  testReorder();
  testRtxRecovery();
  testGiveUp();
  testNackBitmask();
  testWraparound();
  testRtxPaddingAndUnknown();
  testNackIgnored();
  testNackAnswered();
  testHeavyReorderKeepsNack();
  testAdaptiveNackDelay();
  testRestart();
  if (failures) {
    std::printf("%d failure(s)\n", failures);
    return 1;
  }
  std::printf("all tests passed\n");
  return 0;
}
