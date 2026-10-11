#pragma once
#include <string>

namespace vkpcnx::utils {
// Saves the last rendered frame (framebuffer pixels, no window decorations) as
// a PNG. Must be called on the main thread. Returns an empty string on success,
// otherwise the error message.
std::string saveScreenshot(const std::string &path);
} // namespace vkpcnx::utils
