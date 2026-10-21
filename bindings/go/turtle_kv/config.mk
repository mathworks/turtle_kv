BUILD_DIR ?= ./build

include $(BUILD_DIR)/deploy/conandeps.mk

.phony: cflags libs

CONAN_DEFINE_FLAG := -D
CONAN_INCLUDE_DIR_FLAG := -I
CONAN_LIB_DIR_FLAG := -L
CONAN_LIB_FLAG := -l
CONAN_SYSTEM_LIB_FLAG := -l

cflags:
	@echo "$(CONAN_DEFINES) $(CONAN_CXXFLAGS) $(CONAN_INCLUDE_DIRS) -march=native"

libs:
	@echo "$(CONAN_LIB_DIRS) -lturtlekv $(CONAN_LIBS) $(CONAN_LIBS) -lm -lstdc++"
