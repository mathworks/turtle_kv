include build/deploy/conandeps.mk

.phony: cflags libs

CONAN_DEFINE_FLAG := -D
CONAN_INCLUDE_DIR_FLAG := -I
CONAN_LIB_DIR_FLAG := -L

cflags:
	@echo "$(CONAN_DEFINES) $(CONAN_CXXFLAGS) $(CONAN_INCLUDE_DIRS) -march=native"

libs:
	@echo "$(CONAN_LIB_DIRS) $(CONAN_LIBS) -lm -lstdc++"
