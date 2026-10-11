#include "core/platform/home_led.hpp"

#ifdef __SWITCH__

#include <borealis/core/logger.hpp>
#include <cstring>
#include <switch.h>

namespace vkpcnx::platform {

namespace {

// Applies the pattern to every controller that has a HOME LED. hidsysSetNotificationLedPattern
// fails for pads without one (left Joy-Con, third-party pads); that's expected
// and ignored. hidsysInitialize/Exit are refcounted, so this is safe to call
// from any thread without keeping the service open between notifications.
void applyPattern(const HidsysNotificationLedPattern &pattern) {
  Result rc = hidsysInitialize();
  if (R_FAILED(rc)) {
    brls::Logger::warning("HomeLed: hidsysInitialize failed: 0x{:x}", rc);
    return;
  }
  static constexpr HidNpadIdType kNpads[] = {
    HidNpadIdType_Handheld,
    HidNpadIdType_No1,
    HidNpadIdType_No2,
    HidNpadIdType_No3,
    HidNpadIdType_No4,
    HidNpadIdType_No5,
    HidNpadIdType_No6,
    HidNpadIdType_No7,
    HidNpadIdType_No8,
  };
  int lit = 0;
  for (HidNpadIdType npad : kNpads) {
    HidsysUniquePadId pads[2]; // a dual Joy-Con npad has two
    s32 count = 0;
    if (R_FAILED(hidsysGetUniquePadsFromNpad(npad, pads, 2, &count)))
      continue;
    for (s32 i = 0; i < count; i++) {
      if (R_SUCCEEDED(hidsysSetNotificationLedPattern(&pattern, pads[i])))
        lit++;
    }
  }
  brls::Logger::debug("HomeLed: pattern set on {} controller(s)", lit);
  hidsysExit();
}

} // namespace

void blinkHomeLed() {
  // On/off at ~0.6 s per cycle; times are multiples of baseMiniCycleDuration
  // (0x8 = 100 ms). A finite cycle count ending on intensity 0 leaves the LED
  // off afterwards, so nothing has to time it out.
  HidsysNotificationLedPattern p;
  std::memset(&p, 0, sizeof p);
  p.baseMiniCycleDuration = 0x8;
  p.totalMiniCycles = 0x1; // 2 mini cycles
  p.totalFullCycles = 0xF; // 15 × 0.6 s ≈ 9 s
  p.startIntensity = 0x0;
  for (int i = 0; i < 2; i++) {
    p.miniCycles[i].ledIntensity = i == 0 ? 0xF : 0x0;
    p.miniCycles[i].transitionSteps = 0x1;   // 100 ms fade
    p.miniCycles[i].finalStepDuration = 0x2; // 200 ms hold
  }
  applyPattern(p);
}

void clearHomeLed() {
  HidsysNotificationLedPattern p;
  std::memset(&p, 0, sizeof p); // all zero = off
  applyPattern(p);
}

} // namespace vkpcnx::platform

#else

namespace vkpcnx::platform {

void blinkHomeLed() {}
void clearHomeLed() {}

} // namespace vkpcnx::platform

#endif
