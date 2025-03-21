import os

from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMake, CMakeToolchain
from conan.tools.files import copy,save,load
from conan.tools.scm import Git

required_conan_version = ">=2.12.2"

class JungleConan(ConanFile):
    name = "jungle"
    package_type = "library"
    license = "Apache-2.0"
    homepage = "https://github.com/eBay/Jungle"

    generators = "CMakeDeps"

    settings = "os", "compiler", "build_type", "arch"

    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "coverage": [True, False],
        "with_snappy" : [True, False],
        "build_tests": [True, False],
        "build_examples": [True, False]
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_snappy" : False,
        "coverage": False,
        "build_tests": False,
        "build_examples":False
    }

    exports_sources = "CMakeLists.txt", "src/*", "include/*", "cmake/*", "tools/*", "scripts/*", "tests/*", "examples/*", "LICENSE"

    def set_version(self):
        if self.version:
            return

        git = Git(self, folder=self.recipe_folder)
        self.version = git.run(cmd="describe --tags --long")
        if self.version.startswith("v"):
            self.version = self.version[1:]

    def requirements(self):
        self.requires("forestdb/[~1]", transitive_headers=True)
        self.requires("zlib/[~1]")
        if self.options.with_snappy:
            self.requires("snappy/[~1]")

    def _computeCommitHash(self):
        hash_file = os.path.join(self.recipe_folder, "COMMIT_HASH")
        if (os.path.exists(hash_file)):
            hash = load(self,path=hash_file)
            self.output.info(f"Fetched commit hash from {hash_file}")
        else: # we are building from local source, i.e. in editable mode
            git = Git(self, folder=self.recipe_folder)
            hash = git.get_commit()
            diff = git.run(cmd="diff --stat")
            if diff:
                hash +="-dirty"
            self.output.info(f"Fetched commit hash {hash} from local Git in {self.recipe_folder}")
        return hash

    def export(self):
        save(self, path=os.path.join(self.export_folder, "COMMIT_HASH"), content=self._computeCommitHash())

    def layout(self):
        cmake_layout(self, generator="CMakeDeps")
        self.cpp.package.libs = [self.name, "simplelogger"]
        self.cpp.source.includedirs = ["include", "src"]
        self.cpp.package.defines = self.cpp.build.defines = ["_JUNGLE_COMMIT_HASH=%s" % self._computeCommitHash()]


    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["WITH_CONAN"] = True
        tc.variables["CONAN_BUILD_COVERAGE"] = False
        tc.variables["CODE_COVERAGE"] = self.options.coverage
        tc.variables["SNAPPY_OPTION"] = self.options.with_snappy
        tc.variables["BUILD_TESTING"] = self.options.build_tests
        tc.variables["BUILD_EXAMPLES"] = self.options.build_examples
        tc.variables["CMAKE_VERBOSE_MAKEFILE"] = True
        tc.variables["CMAKE_EXPORT_COMPILE_COMMANDS"] = True
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if self.options.build_tests:
            cmake.ctest()
        copy(self, "compile_commands.json", self.build_folder, self.source_folder, keep_path=False)

    def package(self):
        cmake = CMake(self)
        cmake.install()
        assert copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses")), "LICENSE Copy failed"
