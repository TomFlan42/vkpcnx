#include "core/utils/screenshot.hpp"

#if !defined(__SWITCH__) && !defined(_WIN32)
#include <borealis.hpp>
#include <glad/glad.h>
#include <GLFW/glfw3.h>
#include <vector>

#define STB_IMAGE_WRITE_IMPLEMENTATION
#include "extern/stb_image_write.h"

namespace vkpcnx::utils {
std::string saveScreenshot(const std::string &path) {
  GLFWwindow *window = glfwGetCurrentContext();
  if (!window)
    return "no current GL context (not on the main thread?)";

  int w = 0, h = 0;
  glfwGetFramebufferSize(window, &w, &h);
  if (w <= 0 || h <= 0)
    return "empty framebuffer";

  std::vector<unsigned char> px(static_cast<size_t>(w) * h * 3);
  glPixelStorei(GL_PACK_ALIGNMENT, 1);
  glReadPixels(0, 0, w, h, GL_RGB, GL_UNSIGNED_BYTE, px.data());

  // GL rows go bottom-up
  stbi_flip_vertically_on_write(1);
  if (!stbi_write_png(path.c_str(), w, h, 3, px.data(), w * 3))
    return "can't write " + path;
  return {};
}
} // namespace vkpcnx::utils
#endif
