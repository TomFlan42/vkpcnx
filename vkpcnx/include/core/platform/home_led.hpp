#pragma once

// HOME-button notification LED (Joy-Con R, Pro Controller) via hid:sys.
// No-ops on platforms other than the Switch.
namespace vkpcnx::platform {

// Blinks the LED on every connected controller for ~10 s, then it goes dark
// by itself
void blinkHomeLed();

// Stops a blink still in progress
void clearHomeLed();

} // namespace vkpcnx::platform
